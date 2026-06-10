from app.services.mnp_log_ingestion.parsers import BaseLogParser, M3DotNetLogParser

# Strategy map: log format key -> parser class
_LOG_PARSER_REGISTRY: dict[str, type[BaseLogParser]] = {
    "m3_dotnet": M3DotNetLogParser,
}


def get_log_parser(fmt: str = "m3_dotnet") -> BaseLogParser:
    """Provide the correct log parser for the configured format."""
    parser_class = _LOG_PARSER_REGISTRY.get(fmt)
    if parser_class is None:
        raise Exception(f"Log format {fmt!r} not recognized")
    return parser_class()
