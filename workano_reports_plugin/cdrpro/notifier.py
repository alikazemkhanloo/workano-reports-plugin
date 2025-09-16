from wazo_confd import bus, sysconfd


class SurveyNotifier:
    def __init__(self, bus, sysconfd):
        self.bus = bus
        self.sysconfd = sysconfd

    def send_sysconfd_handlers(self):
        pass

    def created(self, survey):
        pass

    def edited(self, survey):
        pass

    def deleted(self, survey):
        pass


def build_survey_notifier():
    return SurveyNotifier(bus, sysconfd)


class QueueFeatureNotifier:
    def __init__(self, bus, sysconfd):
        self.bus = bus
        self.sysconfd = sysconfd

    def send_sysconfd_handlers(self):
        pass

    def created(self, survey):
        pass

    def edited(self, survey):
        pass

    def deleted(self, survey):
        pass


def build_queuefeature_notifier():
    return QueueFeatureNotifier(bus, sysconfd)
