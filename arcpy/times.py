
from datetime import datetime, timedelta

begin_period_1 = datetime(2016, 6, 6)
begin_period_2 = datetime(2016, 9, 5)
period_length = 4
two_hours = timedelta(hours=2)
times = {}
times.update({
   'week%d' % (week + 1): [(
         begin_period_1 - two_hours + timedelta(weeks=week),
         begin_period_1 - two_hours + timedelta(weeks=week + 1)
    )] for week in range(period_length)
})
times.update({
    'week%d' % (week + 5): [(
        begin_period_2 - two_hours + timedelta(weeks=week),
        begin_period_2 - two_hours + timedelta(weeks=week + 1)
    )] for week in range(period_length)
})
times['summer'] = [(
    begin_period_1 - two_hours + timedelta(weeks=period_length),
    begin_period_2 - two_hours
)]
times['all'] = [
    (begin_period_1 - two_hours, begin_period_1 - two_hours + timedelta(weeks=period_length)),
    (begin_period_2 - two_hours, begin_period_2 - two_hours + timedelta(weeks=period_length))
]



query = """SELECT '{name}' AS name, count(track) AS count FROM measurements WHERE {where_clause}"""
where = """(time BETWEEN '{begin!s}' AND '{end!s}')"""
print(' UNION ALL '.join(query.format(name=name, where_clause=' OR '.join(where.format(begin=t[0], end=t[1]) for t in times[name])) for name in times))
