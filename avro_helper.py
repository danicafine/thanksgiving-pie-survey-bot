import argparse, sys
from confluent_kafka import avro, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from uuid import uuid4

respondent_schema = """
{ 
    "name": "respondent",
    "namespace": "com.thanksgiving.survey",
    "type": "record",
    "doc": "Respondent details.",
    "fields": [
        {
            "doc": "Respondent name.",
            "name": "name",
            "type": "string"
        },
        {
            "doc": "Respondent company.",
            "name": "company",
            "type": ["null","string"],
            "default": null
        },
        {
            "doc": "Respondent location.",
            "name": "location",
            "type": ["null","string"],
            "default": null
        }
    ]
}
"""

class Respondent(object):
    """Respondent stores the deserialized Avro record for the Kafka key."""
    # Use __slots__ to explicitly declare all data members.
    __slots__ = [
        "name",
        "company",
        "location"
    ]
    
    def __init__(self, name, company, location):
        self.name     = name
        self.company  = company
        self.location = location

    @staticmethod
    def dict_to_respondent(obj, ctx):
        return Respondent(
                obj['name'],
                obj['company'],    
                obj['location']   
            )

    @staticmethod
    def respondent_to_dict(respondent, ctx):
        return Respondent.to_dict(respondent)

    def to_dict(self):
        return dict(
                    name     = self.name,
                    company  = self.company,
                    location = self.location
                )

    def __hash__(self):
       
        # hash(custom_object)
        return hash((self.name, self.company, self.location))

response_schema = """
{
    "name": "response",
    "namespace": "com.thanksgiving.survey",
    "type": "record",
    "doc": "Individual survey response.",
    "fields": [
        {
            "doc": "Respondent id.",
            "name": "respondent_id",
            "type": "int"
        },
        {
            "doc": "Favorite pie",
            "name": "favorite_pie",
            "type": "string"
        }
    ]
}
"""

class Response(object):
    """Response stores the deserialized Avro record for the Kafka key."""
    # Use __slots__ to explicitly declare all data members.
    __slots__ = [
        "respondent_id", 
        "favorite_pie"
    ]
    
    def __init__(self, respondent_id, favorite_pie):
        self.respondent_id = respondent_id
        self.favorite_pie  = favorite_pie

    @staticmethod
    def dict_to_response(obj, ctx):
        return response(
                obj['respondent_id'],
                obj['favorite_pie']   
            )

    @staticmethod
    def response_to_dict(response, ctx):
        return Response.to_dict(response)

    def to_dict(self):
        return dict(
                    respondent_id = self.respondent_id,
                    favorite_pie  = self.favorite_pie
                )