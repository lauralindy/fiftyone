"""
Dataset sample fields.

| Copyright 2017-2020, Voxel51, Inc.
| `voxel51.com <https://voxel51.com/>`_
|
"""
from bson.binary import Binary
import mongoengine.fields
import numpy as np

import eta.core.image as etai
import eta.core.utils as etau

import fiftyone.core.utils as fou


def parse_field_str(field_str):
    """Parses the string representation of a :class:`Field` generated by
    ``str(field)`` into components that can be passed to
    :meth:`fiftyone.core.dataset.Dataset.add_sample_field`.

    Returns:
        a tuple of

        -   ftype: the :class:`fiftyone.core.fields.Field` class
        -   embedded_doc_type: the
                :class:`fiftyone.core.odm.BaseEmbeddedDocument` type of the
                field, or ``None``
        -   subfield: the :class:`fiftyone.core.fields.Field` class of the
                subfield, or ``None``
    """
    chunks = field_str.strip().split("(", 1)
    ftype = etau.get_class(chunks[0])
    embedded_doc_type = None
    subfield = None
    if len(chunks) > 1:
        param = etau.get_class(chunks[1][:-1])  # remove trailing ")"
        if issubclass(ftype, EmbeddedDocumentField):
            embedded_doc_type = param
        elif issubclass(ftype, (ListField, DictField)):
            subfield = param
        else:
            raise ValueError("Failed to parse field string '%s'" % field_str)

    return ftype, embedded_doc_type, subfield


class Field(mongoengine.fields.BaseField):
    """Base class for :class:`fiftyone.core.sample.Sample` fields."""

    def __str__(self):
        return etau.get_class_name(self)


class ObjectIdField(mongoengine.ObjectIdField, Field):
    """An Object ID field."""

    pass


class UUIDField(mongoengine.UUIDField, Field):
    """A UUID field."""

    pass


class BooleanField(mongoengine.BooleanField, Field):
    """A boolean field."""

    pass


class IntField(mongoengine.IntField, Field):
    """A 32 bit integer field."""

    pass


class FloatField(mongoengine.FloatField, Field):
    """A floating point number field."""

    def validate(self, value):
        try:
            value = float(value)
        except OverflowError:
            self.error("The value is too large to be converted to float")
        except (TypeError, ValueError):
            self.error("%s could not be converted to float" % value)

        if self.min_value is not None and value < self.min_value:
            self.error("Float value is too small")

        if self.max_value is not None and value > self.max_value:
            self.error("Float value is too large")


class StringField(mongoengine.StringField, Field):
    """A unicode string field."""

    pass


class ListField(mongoengine.ListField, Field):
    """A list field that wraps a standard :class:`Field`, allowing multiple
    instances of the field to be stored as a list in the database.

    If this field is not set, its default value is ``[]``.

    Args:
        field (None): an optional :class:`Field` instance describing the
            type of the list elements
    """

    def __init__(self, field=None, **kwargs):
        if field is not None:
            if not isinstance(field, Field):
                raise ValueError(
                    "Invalid field type '%s'; must be a subclass of %s"
                    % (type(field), Field)
                )

        super().__init__(field=field, **kwargs)

    def __str__(self):
        if self.field is not None:
            return "%s(%s)" % (
                etau.get_class_name(self),
                etau.get_class_name(self.field),
            )

        return etau.get_class_name(self)


class DictField(mongoengine.DictField, Field):
    """A dictionary field that wraps a standard Python dictionary.

    If this field is not set, its default value is ``{}``.

    Args:
        field (None): an optional :class:`Field` instance describing the type
            of the values in the dict
    """

    def __init__(self, field=None, **kwargs):
        if field is not None:
            if not isinstance(field, Field):
                raise ValueError(
                    "Invalid field type '%s'; must be a subclass of %s"
                    % (type(field), Field)
                )

        super().__init__(field=field, **kwargs)

    def __str__(self):
        if self.field is not None:
            return "%s(%s)" % (
                etau.get_class_name(self),
                etau.get_class_name(self.field),
            )

        return etau.get_class_name(self)


class VectorField(Field):
    """A one-dimensional array field.

    :class:`VectorField` instances accept lists, tuples, and numpy array
    values. The underlying data is stored as a list in the database and always
    retrieved as a numpy array.
    """

    def to_mongo(self, value):
        if value is None:
            return None

        return np.asarray(value).tolist()

    def to_python(self, value):
        if value is None or isinstance(value, np.ndarray):
            return value

        return np.asarray(value)

    def validate(self, value):
        if isinstance(value, np.ndarray):
            if value.ndim > 1:
                self.error("Only 1D arrays may be used in a vector field")
        elif not isinstance(value, (list, tuple)):
            self.error(
                "Only numpy arrays, lists, and tuples may be used in a "
                "vector field"
            )


class ArrayField(mongoengine.fields.BinaryField, Field):
    """An n-dimensional array field.

    :class:`ArrayField` instances accept numpy array values. The underlying
    data is serialized and stored in the database as zlib-compressed bytes
    generated by ``numpy.save`` and always retrieved as a numpy array.
    """

    def to_mongo(self, value):
        if value is None:
            return None

        bytes = fou.serialize_numpy_array(value)
        return super().to_mongo(bytes)

    def to_python(self, value):
        if value is None or isinstance(value, np.ndarray):
            return value

        return fou.deserialize_numpy_array(value)

    def validate(self, value):
        if not isinstance(value, (np.ndarray, Binary)):
            self.error("Only numpy arrays may be used in an array field")


class ImageLabelsField(Field):
    """A field that stores an ``eta.core.image.ImageLabels`` instance.

    :class:`ImageLabelsField` instances accept ``eta.core.image.ImageLabels``
    instances or serialized dict representations of them. The underlying data
    is stored as a serialized dictionary in the dataset and always retrieved as
    an ``eta.core.image.ImageLabels`` instance.
    """

    def to_mongo(self, value):
        if value is None:
            return None

        return value.serialize()

    def to_python(self, value):
        if value is None or isinstance(value, etai.ImageLabels):
            return value

        return etai.ImageLabels.from_dict(value)

    def validate(self, value):
        if not isinstance(value, (dict, etai.ImageLabels)):
            self.error(
                "Only dicts and `eta.core.image.ImageLabels` instances may be "
                "used in an ImageLabels field"
            )


class FramesField(mongoengine.fields.MapField, Field):
    """A field that stores an ``eta.core.image.ImageLabels`` instance.

    :class:`ImageLabelsField` instances accept ``eta.core.image.ImageLabels``
    instances or serialized dict representations of them. The underlying data
    is stored as a serialized dictionary in the dataset and always retrieved as
    an ``eta.core.image.ImageLabels`` instance.
    """

    def __init__(self, *args, **kwargs):
        from fiftyone.core.frames import FrameSample

        super().__init__(mongoengine.fields.ReferenceField(FrameSample))


class EmbeddedDocumentField(mongoengine.EmbeddedDocumentField, Field):
    """A field that stores instances of a given type of
    :class:`fiftyone.core.odm.BaseEmbeddedDocument` object.

    Args:
        document_type: the :class:`fiftyone.core.odm.BaseEmbeddedDocument` type
            stored in this field
    """

    def __init__(self, document_type, **kwargs):
        #
        # @todo resolve circular import errors in `fiftyone.core.odm.sample`
        # so that this validation can occur here
        #
        # import fiftyone.core.odm as foo
        #
        # if not issubclass(document_type, foo.BaseEmbeddedDocument):
        #     raise ValueError(
        #         "Invalid document type %s; must be a subclass of %s"
        #         % (document_type, foo.BaseEmbeddedDocument)
        #     )
        #

        super().__init__(document_type, **kwargs)

    def __str__(self):
        return "%s(%s)" % (
            etau.get_class_name(self),
            etau.get_class_name(self.document_type),
        )
