from app.services.data_ingestion.pipeline.parsers import BaseParser, PDFLineExtractor, DocxParser, MarkdownParser, HtmlParser

# Strategy map: MIME type → parser class
_PARSER_REGISTRY: dict[str, type[BaseParser]] = {
    "application/pdf": PDFLineExtractor,
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": DocxParser,
    "text/markdown": MarkdownParser,
    "text/plain": MarkdownParser,  # treat plain text as markdown
    "text/html": HtmlParser,
}



def get_parser_for(mime_type:str)->BaseParser:
    """provide the correct parser based on the mime type"""
    parser_class = _PARSER_REGISTRY.get(mime_type) #Just the class name
    if parser_class is None:
        raise Exception(f"Parser type {mime_type} not recognized")

    # instantiate the class and return the object
    return parser_class()


