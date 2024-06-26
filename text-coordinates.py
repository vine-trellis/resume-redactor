# resume stuff

from dataclasses import dataclass
from typing import Optional, List


import io
import re
from pdfminer.high_level import extract_text
from pdfminer.layout import LAParams, LTText, LTChar, LTAnno
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFPageInterpreter, PDFResourceManager
from pdfminer.converter import PDFPageAggregator


@dataclass(unsafe_hash=True)
class TextCoordinates:
    resume_id: int
    redacted: bool
    text: str
    x0: float
    x1: float
    y0: float
    y1: float
    id: Optional[int] = None

class Resume:
    def __init__(
        self,
        link: str | None = None,
        text_coordinates: Optional[List[TextCoordinates]] = None,
        text: Optional[str] = None,
        width: Optional[int] = None,
        height: Optional[int] = None,
        redacted_link: str | None = None,
        redacted_text: str | None = None,
        skip_redaction: bool | None = None,
        show_redacted: bool | None = None,
        redaction_version: int | None = None,
        bytes: bytes | None = None,
        id: Optional[int] = None,
        uuid: Optional[str] = None,
    ):
        self.link = link
        if text_coordinates is None:
            self.text_coordinates = list()
        else:
            self.text_coordinates = text_coordinates
        self.text = text
        self.width = width
        self.height = height
        self.id = id
        self.uuid = uuid

        self.bytes = None

        self.skip_redaction = skip_redaction
        self.redacted_link = redacted_link
        self.redacted_text = redacted_text
        self.show_redacted = show_redacted
        self.redaction_version = redaction_version

        self.events = []

    @classmethod
    def from_bytes(cls, bytes: bytes, **kwargs):
        width, height = resume_measurer(bytes)
        text = parse_resume_text(bytes)
        return cls(width=width, height=height, text=text, **kwargs)

    def set_text_coordinates(self, text_coordinates: List[TextCoordinates]):
        self.text_coordinates = text_coordinates

    def add_text_coordinates(self, text_coordinates: List[TextCoordinates]):
        self.text_coordinates = self.text_coordinates + text_coordinates


def parse_resume_text(blob: bytes) -> str:
    stream = io.BytesIO(blob)
    text = extract_text(stream)
    stream.close()
    text = text.replace("\x00", "")
    return text

def resume_measurer(blob: bytes):
    stream = io.BytesIO(blob)
    pages = PDFPage.get_pages(stream)

    # only get the first page
    page = next(pages, None)
    if page is None:
        # return 0, 0 if no page is found
        return 0, 0

    width = page.mediabox[2]
    height = page.mediabox[3]
    return width, height


def find_text_coordinates(
    bytes: bytes, resume_id: int = None, redacted=False
) -> List[TextCoordinates]:
    stream = io.BytesIO(bytes)
    manager = PDFResourceManager()
    laparams = LAParams()
    dev = PDFPageAggregator(manager, laparams=laparams)
    interpreter = PDFPageInterpreter(manager, dev)
    pages = PDFPage.get_pages(stream)

    # only get the first page
    page = next(pages, None)
    if page is None:
        return []

    interpreter.process_page(page)
    layout = dev.get_result()
    x0, y0, x1, y1, text = -1, -1, -1, -1, ""
    text_coordinates = []
    for textbox in layout:
        if isinstance(textbox, LTText):
            try:
                for line in textbox:
                    for char in line:
                        # If the char is a line-break or an empty space, the word is complete
                        if isinstance(char, LTAnno) or char.get_text() == " ":
                            if x0 != -1:
                                # find all words of at least len >= 1, e.g. "go", "c"
                                words = re.findall(r"(\w+)", text.lower())
                                non_stop_words = [
                                    w for w in words if w not in STOP_WORDS
                                ]
                                if len(non_stop_words) > 0:
                                    text_coordinates.append(
                                        TextCoordinates(
                                            text=" ".join(non_stop_words),
                                            x0=x0,
                                            y0=y0,
                                            x1=x1,
                                            y1=y1,
                                            resume_id=resume_id,
                                            redacted=redacted,
                                        )
                                    )
                            x0, y0, x1, y1, text = -1, -1, -1, -1, ""
                        elif isinstance(char, LTChar):
                            text += char.get_text()
                            if x0 == -1:
                                x0 = char.bbox[0]
                                y0 = char.bbox[1]
                            x1 = char.bbox[2]
                            y1 = char.bbox[3]
            except TypeError:
                continue
    return text_coordinates


def get_resume(uow, uuid, keywords: str | None = None):
    with uow:
        query = uow.session.execute(
            """
            SELECT 
            "resume".id as "id",
            "resume".uuid as "uuid", 
            "resume".link as "link",
            "resume".width as "width", 
            "resume".height as "height"
            FROM "resume"
            WHERE "resume".uuid = :uuid
            """,
            dict(uuid=uuid),
        )
        result = query.one_or_none()
        if result is None:
            return None
        resume = dict(result)

        if keywords is not None:
            safe_keywords = re.sub(r"(\s+)", "|", keywords)
            safe_keywords = re.sub(r"([^\|\w]+)", "", safe_keywords)

            tc_query = uow.session.execute(
                """
                SELECT
                "text_coordinates".text as "text",
                "text_coordinates".x0 as "x0",
                "text_coordinates".x1 as "x1",
                "text_coordinates".y0 as "y0",
                "text_coordinates".y1 as "y1"
                FROM "text_coordinates"
                WHERE "text_coordinates".resume_id = :resume_id
                AND "text_coordinates".tsv @@ to_tsquery('english', :keywords)
                """,
                dict(resume_id=resume.pop("id"), keywords=safe_keywords),
            )
            tc_result = tc_query.all()
            resume["text_coordinates"] = [dict(tc) for tc in tc_result]
        else:
            resume["text_coordinates"] = []

        return resume


STOP_WORDS = [
    "i",
    "me",
    "my",
    "myself",
    "we",
    "our",
    "ours",
    "ourselves",
    "you",
    "your",
    "yours",
    "yourself",
    "yourselves",
    "he",
    "him",
    "his",
    "himself",
    "she",
    "her",
    "hers",
    "herself",
    "it",
    "its",
    "itself",
    "they",
    "them",
    "their",
    "theirs",
    "themselves",
    "what",
    "which",
    "who",
    "whom",
    "this",
    "that",
    "these",
    "those",
    "am",
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "being",
    "have",
    "has",
    "had",
    "having",
    "do",
    "does",
    "did",
    "doing",
    "a",
    "an",
    "the",
    "and",
    "but",
    "if",
    "or",
    "because",
    "as",
    "until",
    "while",
    "of",
    "at",
    "by",
    "for",
    "with",
    "about",
    "against",
    "between",
    "into",
    "through",
    "during",
    "before",
    "after",
    "above",
    "below",
    "to",
    "from",
    "up",
    "down",
    "in",
    "out",
    "on",
    "off",
    "over",
    "under",
    "again",
    "further",
    "then",
    "once",
    "here",
    "there",
    "when",
    "where",
    "why",
    "how",
    "all",
    "any",
    "both",
    "each",
    "few",
    "more",
    "most",
    "other",
    "some",
    "such",
    "no",
    "nor",
    "not",
    "only",
    "own",
    "same",
    "so",
    "than",
    "too",
    "very",
    "s",
    "t",
    "can",
    "will",
    "just",
    "don",
    "should",
    "now",
    "www",
    "inc",
]
