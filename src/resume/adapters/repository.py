import abc

from resume.domain import model
from resume.config import get_current_redaction_version
from sqlalchemy import or_, func


class AbstractRepository(abc.ABC):
    def __init__(self):
        self.seen = set()

    def add(self, resume: model.Resume):
        self._add(resume)
        self.seen.add(resume)

    def get(self, id) -> model.Resume:
        resume = self._get(id)
        if resume:
            self.seen.add(resume)
        return resume

    def get_by_uuid(self, uuid) -> model.Resume:
        resume = self._get_by_uuid(uuid)
        if resume:
            self.seen.add(resume)
        return resume

    def get_without_redacted(self) -> model.Resume:
        resume = self._get_without_redacted()
        if resume:
            self.seen.add(resume)
        return resume

    @abc.abstractmethod
    def _add(self, resume: model.Resume):
        raise NotImplementedError

    @abc.abstractmethod
    def _get(self, id) -> model.Resume:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_by_uuid(self, uuid) -> model.Resume:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_without_redacted(self) -> model.Resume:
        raise NotImplementedError


DEFAULT_CURRENT_REDACTION_VERSION = get_current_redaction_version()


class SqlAlchemyRepository(AbstractRepository):
    def __init__(
        self, session, current_redaction_version=DEFAULT_CURRENT_REDACTION_VERSION
    ):
        super().__init__()
        self.session = session
        self.current_redaction_version = current_redaction_version

    def _add(self, posting: model.Resume):
        self.session.add(posting)

    def _get(self, id):
        return self.session.query(model.Resume).filter_by(id=id).first()

    def _get_by_uuid(self, uuid):
        return self.session.query(model.Resume).filter_by(uuid=uuid).first()

    def _get_without_redacted(self) -> model.Resume:
        return (
            self.session.query(model.Resume)
            .filter(
                or_(
                    model.Resume.skip_redaction != True,
                    model.Resume.skip_redaction == None,
                    model.Resume.redaction_version < self.current_redaction_version,
                    model.Resume.redaction_version == None,
                )
            )
            .order_by(func.random())
            .first()
        )
