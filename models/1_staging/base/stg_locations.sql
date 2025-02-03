with

base as (
    select
        LocationId as location_id,
        LocationName as location_name
    from {{ source('raw_data', 'src_locations') }}
)

select * from base