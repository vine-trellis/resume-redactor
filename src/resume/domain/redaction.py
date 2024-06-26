import abc
import fitz
import io
from pdfminer.high_level import extract_text
import scrubadub
import scrubadub_stanford
import nltk
import os

STANFORD_DATA_PATH = "./nltk/stanford-ner-4.0.0"

STANFORD_CLASSIFIER_PATH = os.path.join(
    STANFORD_DATA_PATH,
    "classifiers",
    "english.all.3class.distsim.crf.ser.gz",
)
STANFORD_NER_JAR_PATH = os.path.join(STANFORD_DATA_PATH, "stanford-ner.jar")


class CachedStanfordEntityDetector(scrubadub_stanford.detectors.StanfordEntityDetector):
    def __init__(
        self,
        enable_person: bool = True,
        enable_organization: bool = True,
        enable_location: bool = False,
        **kwargs
    ):
        """Initialise the ``Detector``.

        :param name: Overrides the default name of the :class:``Detector``
        :type name: str, optional
        :param locale: The locale of the documents in the format: 2 letter lower-case language code followed by an
                       underscore and the two letter upper-case country code, eg "en_GB" or "de_CH".
        :type locale: str, optional
        """
        super().__init__(enable_person, enable_organization, enable_location, **kwargs)
        self.stanford_tagger = nltk.tag.StanfordNERTagger(
            STANFORD_CLASSIFIER_PATH, STANFORD_NER_JAR_PATH
        )


class RedactionStrategy(abc.ABC):
    @abc.abstractmethod
    def apply(self, bytes: bytes) -> bytes:
        raise NotImplementedError


class Top30Percent(RedactionStrategy):
    def apply(self, bytes: bytes) -> bytes:
        pdf = fitz.Document(stream=bytes, filetype="pdf")
        for page in pdf.pages():
            # clean the resume
            page.clean_contents()

            # strip top 30% of page
            page_bound = page.bound()
            top_thirty = page_bound.transform(
                fitz.Matrix(fitz.Identity).pretranslate(0, -page_bound.height * 0.7)
            )
            for word in page.get_text("words", clip=top_thirty):
                page.add_redact_annot(fitz.Rect(word[:4]), fill=(0, 0, 0))

            page.apply_redactions()
        out_stream = io.BytesIO()
        pdf.save(out_stream, deflate=True, garbage=3)
        pdf.close()
        out_stream.seek(0, 0)
        out_bytes = out_stream.read()
        out_stream.close()
        return out_bytes


class Bottom10Percent(RedactionStrategy):
    def apply(self, bytes: bytes) -> bytes:
        pdf = fitz.Document(stream=bytes, filetype="pdf")
        for page in pdf.pages():
            # clean the resume
            page.clean_contents()

            # strip bottom 10% of page
            page_bound = page.bound()
            bottom_ten = page_bound.transform(
                fitz.Matrix(fitz.Identity).pretranslate(0, page_bound.height * 0.9)
            )
            for word in page.get_text("words", clip=bottom_ten):
                page.add_redact_annot(fitz.Rect(word[:4]), fill=(0, 0, 0))

            page.apply_redactions()
        out_stream = io.BytesIO()
        pdf.save(out_stream, deflate=True, garbage=3)
        pdf.close()
        out_stream.seek(0, 0)
        out_bytes = out_stream.read()
        out_stream.close()
        return out_bytes


class LinkRedactor(RedactionStrategy):
    def apply(self, bytes: bytes) -> bytes:
        pdf = fitz.Document(stream=bytes, filetype="pdf")
        for page in pdf.pages():
            # clean the resume
            page.clean_contents()

            # strip links
            for link in page.get_links():
                page.delete_link(link)
                page.add_redact_annot(link["from"], fill=(0, 0, 0))

            page.apply_redactions()
        out_stream = io.BytesIO()
        pdf.save(out_stream, deflate=True, garbage=3)
        pdf.close()
        out_stream.seek(0, 0)
        out_bytes = out_stream.read()
        out_stream.close()
        return out_bytes


class ImageRedactor(RedactionStrategy):
    def apply(self, bytes: bytes) -> bytes:
        pdf = fitz.Document(stream=bytes, filetype="pdf")
        for page in pdf.pages():
            # clean the resume
            page.clean_contents()

            # redact images
            for image in page.get_images():
                redacted_rects = page.get_image_rects(image)
                for rect in redacted_rects:
                    page.add_redact_annot(rect, fill=(0, 0, 0))

            page.apply_redactions()
        out_stream = io.BytesIO()
        pdf.save(out_stream, deflate=True, garbage=3)
        pdf.close()
        out_stream.seek(0, 0)
        out_bytes = out_stream.read()
        out_stream.close()
        return out_bytes


class MetadataRedactor(RedactionStrategy):
    def apply(self, bytes: bytes) -> bytes:
        pdf = fitz.Document(stream=bytes, filetype="pdf")
        pdf.set_metadata({})
        out_stream = io.BytesIO()
        pdf.save(out_stream, deflate=True, garbage=3)
        pdf.close()
        out_stream.seek(0, 0)
        out_bytes = out_stream.read()
        out_stream.close()
        return out_bytes


class StanfordRedactor(RedactionStrategy):
    def __init__(self, **kwargs):
        self.stanford_entity_detector_kwargs = kwargs

    def _get_text(self, bytes: bytes):
        stream = io.BytesIO(bytes)
        text = extract_text(stream)
        stream.close()
        text = text.replace("\x00", "")
        return text

    def _find_dirty_words(self, text) -> list[str]:
        scrubber = scrubadub.Scrubber()
        scrubber.add_detector(
            CachedStanfordEntityDetector(**self.stanford_entity_detector_kwargs)
        )
        filth_list = list(scrubber.iter_filth(text, document_name=None))
        filth_list = scrubber._post_process_filth_list(filth_list)
        dirty_words = [filth.text for filth in filth_list]
        return dirty_words

    def apply(self, bytes: bytes) -> bytes:
        dirty_text = self._get_text(bytes)
        dirty_words = self._find_dirty_words(dirty_text)

        pdf = fitz.Document(stream=bytes, filetype="pdf")
        for page in pdf.pages():
            # clean the resume
            page.clean_contents()

            # redact words
            for dirty_word in dirty_words:
                redacted_quads = page.search_for(dirty_word, quads=True)
                for quad in redacted_quads:
                    page.add_redact_annot(quad, fill=(0, 0, 0))
            page.apply_redactions()
        out_stream = io.BytesIO()
        pdf.save(out_stream, deflate=True, garbage=3)
        pdf.close()
        out_stream.seek(0, 0)
        out_bytes = out_stream.read()
        out_stream.close()
        return out_bytes
