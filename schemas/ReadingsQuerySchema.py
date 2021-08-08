import time

from marshmallow import fields, Schema
from marshmallow.validate import OneOf, Range

SENSOR_TYPES = ["temperature", "humidity"]


class GetReadingsQuerySchema(Schema):
    type = fields.Str(strict=True, validate=OneOf(SENSOR_TYPES))
    start = fields.Int()
    end = fields.Int()


class CreateReadingsQuerySchema(Schema):
    type = fields.String(required=True, validate=OneOf(SENSOR_TYPES))
    value = fields.Integer(required=True, validate=[Range(min=0, max=100, error="Value must be in between 0 to 100")])
    date_created = fields.Integer(required=False, default=int(time.time()))


class GetMaxReadingQuerySchema(Schema):
    type = fields.Str(required=True, strict=True, validate=OneOf(SENSOR_TYPES))
    start = fields.Int()
    end = fields.Int()


class GetQuartilesReadingQuerySchema(Schema):
    type = fields.Str(required=True, strict=True, validate=OneOf(SENSOR_TYPES))
    start = fields.Int(required=True)
    end = fields.Int(required=True)


class GetSummaryReadingQuerySchema(Schema):
    type = fields.Str(strict=True, validate=OneOf(SENSOR_TYPES))
    start = fields.Int()
    end = fields.Int()
