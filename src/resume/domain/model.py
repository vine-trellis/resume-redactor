from dataclasses import dataclass
from functools import reduce
from typing import Optional, List

import io
import re
import logging
from pdfminer.high_level import extract_text
from pdfminer.layout import LAParams, LTText, LTChar, LTAnno
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFPageInterpreter, PDFResourceManager
from pdfminer.converter import PDFPageAggregator
from resume.domain.redaction import RedactionStrategy
import scrubadub
import scrubadub_stanford


from .consts import STOP_WORDS

logging.getLogger("pdfminer").setLevel(logging.WARNING)


@dataclass(unsafe_hash=True)
class Prospect:
    uuid: Optional[str]
    id: Optional[int]


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


def find_dirty_words(text):
    scrubber = scrubadub.Scrubber()
    scrubber.add_detector(
        scrubadub_stanford.detectors.StanfordEntityDetector(enable_person=True)
    )
    filth_list = list(scrubber.iter_filth(text, document_name=None))
    filth_list = scrubber._post_process_filth_list(filth_list)
    dirty_words = [filth.text for filth in filth_list]
    return dirty_words


def redact_pdf(bytes: bytes, redaction_strategies: List[RedactionStrategy]):
    out_bytes = reduce(lambda acc, strat: strat.apply(acc), redaction_strategies, bytes)
    return out_bytes


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
