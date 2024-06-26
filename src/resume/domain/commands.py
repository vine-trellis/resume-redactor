from dataclasses import dataclass, field


class Command:
    pass


@dataclass
class CreateResume(Command):
    prospect_uuid: str
    uuid: str
    resume_bytes: bytes = field(repr=False)


@dataclass
class AttachTextCoordinates(Command):
    uuid: str


@dataclass
class AttachRedactedTextCoordinates(Command):
    uuid: str


@dataclass
class KickoffResumeRedaction(Command):
    pass


@dataclass
class RedactResume(Command):
    uuid: str
