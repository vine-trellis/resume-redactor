import marshmallow as ma


class TextCoordinates(ma.Schema):
    text = ma.fields.String()
    x0 = ma.fields.Float()
    x1 = ma.fields.Float()
    y0 = ma.fields.Float()
    y1 = ma.fields.Float()


class Resume(ma.Schema):
    link = ma.fields.String()
    text_coordinates = ma.fields.Nested(TextCoordinates(many=True))
    width = ma.fields.Integer()
    height = ma.fields.Integer()
    uuid = ma.fields.String()
