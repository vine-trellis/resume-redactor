from dataclasses import dataclass


class Event:
    pass


@dataclass
class ResumeCreated(Event):
    uuid: str


@dataclass
class ResumeRedacted(Event):
    uuid: str
