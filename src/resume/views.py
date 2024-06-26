from typing import List, Literal
import re


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
