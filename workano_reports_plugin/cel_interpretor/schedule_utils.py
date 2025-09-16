from xivo_dao.alchemy.schedule import Schedule as ScheduleModel
from wazo_agid.schedule import (
    AlwaysOpenedSchedule,
    Schedule,
    ScheduleAction,
    SchedulePeriodBuilder,
)


def get_schedule_mapper(schedule: ScheduleModel)-> Schedule:
    """Map an ORM ScheduleModel to a wazo_agid Schedule.

    - If schedule is None or commented, return AlwaysOpenedSchedule.
    - Build default action from fallback_action fields.
    - Convert Schedule.periods (ScheduleTime objects) into opened/closed periods
      using SchedulePeriodBuilder and ScheduleAction.
    """
    if not schedule:
        return AlwaysOpenedSchedule()

    # If schedule is commented/disabled, treat as always opened
    if getattr(schedule, 'commented', 0) != 0:
        return AlwaysOpenedSchedule()

    schedule_id = getattr(schedule, 'id', None)
    timezone = getattr(schedule, 'timezone', None)

    default_action = ScheduleAction(
        getattr(schedule, 'fallback_action', None),
        getattr(schedule, 'fallback_actionid', None),
        getattr(schedule, 'fallback_actionargs', None),
    )

    opened_periods = []
    closed_periods = []

    # schedule.periods should be preloaded (selectinload in DAO)
    for period in getattr(schedule, 'periods', []) or []:
        try:
            pb = SchedulePeriodBuilder()
            pb.hours(getattr(period, 'hours', None))
            pb.weekdays(getattr(period, 'weekdays', None))
            pb.days(getattr(period, 'monthdays', None))
            pb.months(getattr(period, 'months', None))

            if getattr(period, 'mode', None) == 'opened':
                opened_periods.append(pb.build())
            else:
                action = ScheduleAction(
                    getattr(period, 'action', None),
                    getattr(period, 'actionid', None),
                    getattr(period, 'actionargs', None),
                )
                pb.action(action)
                closed_periods.append(pb.build())
        except Exception:
            # skip malformed period entries
            continue

    return Schedule(opened_periods, closed_periods, default_action, timezone)