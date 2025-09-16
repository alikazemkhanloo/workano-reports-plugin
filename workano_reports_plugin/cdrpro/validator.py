from wazo_confd.helpers.validator import Validator, ValidationGroup


class SurveyValidator(Validator):
    def validate(self, model):
        return


def build_survey_validator():
    survey_validator = SurveyValidator()
    return ValidationGroup(create=[survey_validator], edit=[survey_validator])


class QueueFeatureValidator(Validator):
    def validate(self, model):
        return


def build_queuefeature_validator():
    queuefeature_validator = QueueFeatureValidator()
    return ValidationGroup(create=[queuefeature_validator], edit=[queuefeature_validator])
