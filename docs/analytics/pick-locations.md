# Pick locations and zones (chunk 129)

What the pick releases page and the assistant show about locations, and how it is declared.

## The four locations around a pick

| location | source | joined by |
| --- | --- | --- |
| designated | `ListPickLinesByUser` response list, element `Location` | release (`ReportingNumber`), lookup `pick line` |
| suggested | `GetOldestItemBalanceAPI` `resp.Location` | same user and item, 3 minutes before the release |
| checked | `GetSummarisedBalanceDetails` / `IsStockInLocation` `Location` | same user and item, 3 minutes before the release |
| actual | `ConfirmPickLine` `FromLocation` | the release itself |

The zone of a location comes from `GetSummarisedBalanceDetails` (`resp.StockZone`), lookup `location`.
A location seen in two zones keeps both, written `A1 | C1` (conflict rule `all_values`), and is flagged on the page.

## Settled values added to `pick_release`

`from_location` (first_text of FromLocation), `designated_location` and the two zones (lookup rules), `from_designated` and `same_zone` (flags), `lookups`, `empty_checks`, `locations_checked`, `suggested_location`, `picked_from_checked` (nearby rules, window 180 s, matched on user and item), `followed_suggestion` (flag).
A release whose pick-list line was never seen has no designated location, and its flags are unknown rather than 0.

## Declarations (tmp-live)

Lookups, then a backfill of each (the pick-line backfill reads raw entries, kept 60 days):

```
POST /api/v1/analytics/lookups
{"name":"location","key_field":"attr:Location","description":"zone and type of a warehouse location",
 "attributes":[
  {"name":"StockZone","on_conflict":"all_values","sources":[{"method":"GetSummarisedBalanceDetails","key_field":"resp.Location","value_field":"resp.StockZone"}]},
  {"name":"LocationType","on_conflict":"all_values","sources":[{"method":"GetSummarisedBalanceDetails","key_field":"resp.Location","value_field":"resp.LocationType"}]}]}
POST /api/v1/analytics/lookups/location/backfill?days=60

POST /api/v1/analytics/lookups
{"name":"pick line","key_field":"attr:ReportingNumber","description":"a pick-list line's designated location",
 "attributes":[{"name":"Location","sources":[{"method":"ListPickLinesByUser","key_field":"ReportingNumber","value_field":"Location","list":true}]}]}
POST /api/v1/analytics/lookups/pick%20line/backfill?days=60
```

Chunks 143 to 150 (deliveries at risk, `docs/analytics-at-risk.md`) extend the `pick line` lookup with three more attributes from the same list source, so the number of lines a delivery expects is known before picking starts:

```
PATCH /api/v1/analytics/lookups/pick%20line
{"attributes":[
  {"name":"Location","sources":[{"method":"ListPickLinesByUser","key_field":"ReportingNumber","value_field":"Location","list":true}]},
  {"name":"DeliveryNumber","sources":[{"method":"ListPickLinesByUser","key_field":"ReportingNumber","value_field":"DeliveryNumber","list":true}]},
  {"name":"LineStatus","stable":false,"on_conflict":"latest_wins","sources":[{"method":"ListPickLinesByUser","key_field":"ReportingNumber","value_field":"LineStatus","list":true}]},
  {"name":"ExpectedQty","sources":[{"method":"ListPickLinesByUser","key_field":"ReportingNumber","value_field":"ExpectedQty","list":true}]}]}
POST /api/v1/analytics/lookups/pick%20line/backfill?days=60
```

The element field is `ExpectedQty`, not `ExpectedQuantity`; the list element spelling was verified on a live response on 2026-10-02.

Then `PATCH /api/v1/analytics/settlements/pick_release` with the existing `values` plus the twelve above (the page's docs payload is `pick_release_values.json` next to this file). The patch rebuilds every release under the new rules.

## Measured on 2026-09-30 before switching on (394 releases, read-only run of the new rules)

307 (77.9%) from the designated location, 24 elsewhere in the same zone, 63 in another zone (almost all JIT); 83 of 87 off-location picks came from a location the picker had checked; 1.89 stock checks per release, 14 of them empty; the oldest-stock suggestion was followed 150 of 150 times; no location was seen in two zones over 14 days.
