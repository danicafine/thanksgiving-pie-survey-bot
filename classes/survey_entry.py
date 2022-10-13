class SurveyEntry(object):
    __slots__ = [
        "survey_id",
        "user_id",
        "response"
    ]

    @staticmethod
    def get_schema():
        with open('./avro/survey_entry.avsc', 'r') as handle:
            return handle.read()
    
    def __init__(self, survey_id, user_id, response):
        self.survey_id = survey_id
        self.user_id   = user_id
        self.response  = response

    @staticmethod
    def dict_to_entry(obj, ctx=None):
        return SurveyEntry(
                obj['survey_id'],
                obj['user_id'],    
                obj['response'],    
            )

    @staticmethod
    def entry_to_dict(entry, ctx=None):
        return SurveyEntry.to_dict(entry)

    def to_dict(self):
        return dict(
                    survey_id = self.survey_id,
                    user_id   = self.user_id,
                    response  = self.response
                )