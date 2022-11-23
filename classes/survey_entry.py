class SurveyEntry(object):
    __slots__ = [
        "survey_id",
        "user_id",
        "name",
        "company",
        "location",
        "response"
    ]

    @staticmethod
    def get_schema():
        with open('./avro/survey_entry.avsc', 'r') as handle:
            return handle.read()
    
    def __init__(self, survey_id, user_id, name, company, location, response):
        self.survey_id = survey_id
        self.user_id   = user_id
        self.name      = name
        self.company   = company
        self.location  = location
        self.response  = response

    @staticmethod
    def dict_to_entry(obj, ctx=None):
        return SurveyEntry(
                obj['survey_id'],
                obj['user_id'], 
                obj['name'],
                obj['company'],
                obj['location'],     
                obj['response'],    
            )

    @staticmethod
    def entry_to_dict(entry, ctx=None):
        return SurveyEntry.to_dict(entry)

    def to_dict(self):
        return dict(
                    survey_id = self.survey_id,
                    user_id   = self.user_id,
                    name      = self.name,
                    company   = self.company,
                    location  = self.location,
                    response  = self.response
                )