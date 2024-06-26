from uuid import uuid4
from flask import Flask, request
from flask_cors import CORS
from flask.views import MethodView
from flask_smorest import Api, Blueprint
from flask_smorest.fields import Upload
import marshmallow as ma

from common.entry_points.flask_middleware import scopes_required
from common.entry_points.wsgi_middleware import ClaimsInjectorMiddleware
from common.adapters.schemas import SingletonSchema
from common.entry_points.error_handler import error_handler
from resume import bootstrap, views
from resume.domain import commands

from resume.adapters import schemas

app = Flask(__name__)
CORS(app, supports_credentials=True)
app.wsgi_app = ClaimsInjectorMiddleware(app.wsgi_app)
app.url_map.strict_slashes = False
app.register_error_handler(Exception, error_handler)

app.config["API_TITLE"] = "resume api"
app.config["API_VERSION"] = "v1"
app.config["OPENAPI_VERSION"] = "3.0.2"
api = Api(app)


bus = bootstrap.bootstrap()
blp = Blueprint("resume", "resumes", description="Operations on resumes")


@blp.route("/healthcheck")
def healthcheck():
    return "OK", 200


class CreateResumeSchema(ma.Schema):
    prospect_uuid = ma.fields.String()
    file = Upload()


@blp.route("/")
class ResumesPlural(MethodView):
    decorators = [scopes_required(["Student"])]

    # @blp.arguments(CreateResumeSchema, location="form")
    # @blp.arguments(CreateResumeSchema, location="files")
    @blp.response(201, SingletonSchema(schemas.Resume))
    def post(self):
        prospect_uuid = request.form.get("prospect_uuid")
        raw_resume = request.files.get("resume").read()
        uuid = str(uuid4())
        cmd = commands.CreateResume(
            prospect_uuid=prospect_uuid,
            resume_bytes=raw_resume,
            uuid=uuid,
        )
        bus.handle(cmd)
        # bus.handle(commands.AttachTextCoordinates(uuid=uuid))
        resume = views.get_resume(bus.uow, uuid)
        rv = dict()
        rv["data"] = resume
        return rv, 201


class GetResumeSchema(ma.Schema):
    keywords = ma.fields.String()


@blp.route("/<resume_uuid>")
class ResumesSingular(MethodView):
    @blp.arguments(GetResumeSchema, location="query")
    @blp.response(200, SingletonSchema(schemas.Resume))
    def get(self, get_data, resume_uuid):
        resume = views.get_resume(
            bus.uow, resume_uuid, keywords=get_data.get("keywords")
        )
        rv = dict()
        rv["data"] = resume
        return rv, 201


api.register_blueprint(blp, url_prefix="/resumes")
