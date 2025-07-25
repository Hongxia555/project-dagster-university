import dagster as dg

from dagster_essentials.assets import metrics, trips, requests
from dagster_essentials.resources import database_resource
from dagster_essentials.jobs import trip_update_job, weekly_update_job, adhoc_request_job
from dagster_essentials.schedules import trip_update_schedule, weekly_update_schedule
from dagster_essentials.sensors import adhoc_request_sensor


trip_assets = dg.load_assets_from_modules([trips])
metric_assets = dg.load_assets_from_modules(
    modules=[metrics], 
    group_name='metrics')
request_assets = dg.load_assets_from_modules(
    modules=[requests], 
    group_name="requests")
all_jobs = [trip_update_job, weekly_update_job, adhoc_request_job]
all_schedules = [trip_update_schedule, weekly_update_schedule]
all_sensors = [adhoc_request_sensor]




defs = dg.Definitions(
    assets=[*trip_assets, *metric_assets, *request_assets],
    resources={
        "database": database_resource,
    },
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors


)



