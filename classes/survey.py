class Survey(object):
    __slots__ = [
        "survey_id",
        "question",
        "summary",
        "options",
        "enabled"
    ]

    @staticmethod
    def get_schema():
        with open('./avro/survey.avsc', 'r') as handle:
            return handle.read()
    
    def __init__(self, survey_id, question, summary, options, enabled):
        self.survey_id = survey_id
        self.question  = question
        self.summary   = summary
        self.options   = options
        self.enabled   = enabled

    @staticmethod
    def dict_to_survey(obj, ctx=None):
        return Survey(
                obj['survey_id'],
                obj['question'], 
                obj['summary'],
                obj['options'],
                obj['enabled']   
            )

    @staticmethod
    def survey_to_dict(entry, ctx=None):
        return Survey.to_dict(entry)

    def to_dict(self):
        return dict(
                    survey_id = self.survey_id,
                    question  = self.question,
                    summary   = self.summary,
                    options   = self.options,
                    enabled   = self.enabled
                )