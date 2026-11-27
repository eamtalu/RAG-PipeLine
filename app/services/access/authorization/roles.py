"""A customer's own roles (program step 4, slice 3).

The matrix file names six roles. A customer may rename or retire any of them and add its own;
a new role starts as a copy of an existing role's permissions. Capabilities stay defined by
code, because a capability only means something where the code checks it. Everything that
needs to know "which roles does this customer have, and what are they called" asks here.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.access import (AccessCustomerRole, AccessRole, AccessRoleCapability,
                                           TeamsUser)
from app.services.access.authorization import matrix as matrix_file

#: The role that sets up everything else; a customer without it could lock itself out.
ADMIN = "admin"
MAX_NAME = 64
_CODE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")


@dataclass(frozen=True)
class RoleInfo:
    """One role as a customer has it."""

    code: str
    name: str
    sort_order: int
    is_active: bool
    is_built_in: bool


def code_of(name: str) -> str:
    """A plain code for a new role, made from its name.

    >>> code_of("Night shift lead"), code_of("  Goods-In / QA  "), code_of("9 to 5")
    ('night_shift_lead', 'goods_in_qa', 'role_9_to_5')
    """
    slug = re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")[:28]
    if not slug:
        raise ValueError("a role needs a name with at least one letter or digit")
    return slug if slug[0].isalpha() else f"role_{slug}"[:32]


async def customer_roles(db: AsyncSession, customer_code: str) -> list[RoleInfo]:
    """Every role of one customer, retired ones included: the built-in six in the file's order
    (with this customer's name and state where it changed them), then the ones it added.

    (untouched customer) -> the six ; (renamed supervisor, added night_lead) -> six + one
    """
    own = {row.code: row for row in (await db.execute(
        select(AccessCustomerRole).where(AccessCustomerRole.customer_code == customer_code)
    )).scalars()}
    built_in = matrix_file.load().roles
    listed = [RoleInfo(r.code, own[r.code].name if r.code in own else r.name, r.sort_order,
                       own[r.code].is_active if r.code in own else True, True)
              for r in built_in]
    codes = {r.code for r in built_in}
    added = sorted((row for code, row in own.items() if code not in codes),
                   key=lambda row: (row.sort_order, row.name.lower()))
    return listed + [RoleInfo(r.code, r.name, r.sort_order, r.is_active, False) for r in added]


async def role_names(db: AsyncSession, customer_code: str) -> dict[str, str]:
    """{code: name} for one customer, retired roles included (people may still carry one)."""
    return {r.code: r.name for r in await customer_roles(db, customer_code)}


async def people_per_role(db: AsyncSession, customer_code: str) -> dict[str, int]:
    """How many people who are not deactivated carry each role: {"supervisor": 3}."""
    rows = (await db.execute(
        select(TeamsUser.role_code, func.count()).where(
            TeamsUser.customer_code == customer_code, TeamsUser.status != "deactivated")
        .group_by(TeamsUser.role_code))).all()
    return {code: int(count) for code, count in rows}


async def set_role(db: AsyncSession, customer_code: str, *, name: str, code: str | None = None,
                   copy_from: str | None = None, actor: str | None = None) -> tuple[RoleInfo, bool]:
    """Add a role (no code given) or rename one; the role and whether it is new.

    (name "Night shift lead", copy_from "shift_lead") -> (the new role, True), its cells copied
    ; (code "supervisor", name "Team leader") -> (renamed, False) ; (a taken name) -> ValueError
    """
    name = (name or "").strip()
    if not name or len(name) > MAX_NAME:
        raise ValueError(f"a role needs a name of at most {MAX_NAME} characters")
    existing = await customer_roles(db, customer_code)
    by_code = {r.code: r for r in existing}
    taken = {r.name.lower(): r.code for r in existing}
    if code is None:
        # Step 1: A new role must start from an existing one, so that it never begins with
        # permissions nobody chose
        if copy_from not in by_code:
            raise ValueError("a new role must start from one of the customer's roles")
        code = code_of(name)
        if name.lower() in taken or code in by_code:
            raise ValueError(f"there is already a role called {name}")
        await db.execute(pg_insert(AccessRole).values(code=code, name=name, sort_order=100)
                         .on_conflict_do_nothing(index_elements=[AccessRole.code]))
        order = max([r.sort_order for r in existing if not r.is_built_in] or [100]) + 1
        db.add(AccessCustomerRole(customer_code=customer_code, code=code, name=name,
                                  sort_order=order, copied_from=copy_from, updated_by=actor))
        cells = (await db.execute(select(AccessRoleCapability).where(
            AccessRoleCapability.customer_code == customer_code,
            AccessRoleCapability.role_code == copy_from))).scalars().all()
        db.add_all([AccessRoleCapability(customer_code=customer_code, role_code=code,
                                         capability_code=cell.capability_code, state=cell.state,
                                         updated_by=actor) for cell in cells])
        await db.flush()
        return RoleInfo(code, name, order, True, False), True
    if code not in by_code:
        raise ValueError(f"{code} is not a role of this customer")
    if taken.get(name.lower(), code) != code:
        raise ValueError(f"there is already a role called {name}")
    row = await _own_row(db, customer_code, by_code[code], actor)
    row.name = name
    await db.flush()
    return RoleInfo(code, name, by_code[code].sort_order, row.is_active,
                    by_code[code].is_built_in), False


async def set_active(db: AsyncSession, customer_code: str, code: str, is_active: bool, *,
                     actor: str | None = None) -> RoleInfo:
    """Retire a role or bring it back.

    (executive with nobody) -> retired ; (supervisor with 3 people) -> ValueError saying so
    ; (admin) -> ValueError, always
    """
    by_code = {r.code: r for r in await customer_roles(db, customer_code)}
    if code not in by_code:
        raise ValueError(f"{code} is not a role of this customer")
    role = by_code[code]
    if not is_active:
        if code == ADMIN:
            raise ValueError("the Admin role cannot be retired: nobody could set up access")
        people = (await people_per_role(db, customer_code)).get(code, 0)
        if people:
            raise ValueError(f"{role.name} still has {people} active "
                             f"{'person' if people == 1 else 'people'}; give them another "
                             "role first")
    row = await _own_row(db, customer_code, role, actor)
    row.is_active = is_active
    await db.flush()
    return RoleInfo(code, role.name, role.sort_order, is_active, role.is_built_in)


def check_code(code: str) -> None:
    """Refuse something that could never be a role code, before it reaches a query."""
    if not _CODE.match(code or ""):
        raise ValueError(f"{code!r} is not a role code")


async def _own_row(db: AsyncSession, customer_code: str, role: RoleInfo,
                   actor: str | None) -> AccessCustomerRole:
    """The customer's own row for a role, made on first change for a built-in one."""
    row = await db.get(AccessCustomerRole, (customer_code, role.code))
    if row is None:
        row = AccessCustomerRole(customer_code=customer_code, code=role.code, name=role.name,
                                 sort_order=role.sort_order, is_active=role.is_active)
        db.add(row)
    row.updated_by = actor
    return row
