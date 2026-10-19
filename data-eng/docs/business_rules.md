# Business rules

This document describes the definitions and rules used to build the punctuality indicators. Each rule has a status:

- ✅ **Implemented**: in the code.
- 🟡 **Validated**: decided, not implemented yet.
- ❓ **Open**: to be decided.

## 1. Vocabulary

| Term | Definition | Example |
| --- | --- | --- |
| **Train** | One trip × one service day. This is the counting unit of every indicator. | Train 862402 on Monday 12/10 |
| **Train number** | Identifier that repeats from one day to the next, with no date. This is what alerts refer to. | `OCESN862402F` |
| **Planned train** | Train present in that day's theoretical GTFS: its `service_id` (`trips`) has the date in `calendar_dates`. | — |
| **Alert** | Message from the GTFS-RT *service alerts* feed. It lists train numbers and one active period. | "Votre train est retardé. Cause : …" |

To get the train number from a GTFS `trip_id`, cut it after the first letter that follows the digits:
`OCESN862402F1187_F:TER:…` → `OCESN862402F`.

Announced trains are built by [`int_planned_trains`](../dbt_data_pipeline/models/intermediate/int_planned_trains.sql), one row per train of the run date with its [train status](#train-status). ✅ It is the denominator of the indicators: the real-time feed and the alerts only mention trains with something to report. `trips` and `calendar_dates` are joined within each GTFS version, and `calendar_dates` is read with `hive_partitioning = false`: otherwise DuckDB replaces its `date` column (service day) with the `date=` folder value.

## 2. Indicators

| # | Indicator | Rule | Status |
| --- | --- | --- | --- |
| 1 | Delay volume | Sum of the arrival delay at the **effective terminus**: the last stop actually served (not `SKIPPED`). A train whose planned terminus is skipped is still counted, at its effective terminus, and flagged `is_limited` for the future partial-cancellation view. Early arrivals count as 0: `GREATEST(delay, 0)`. The last known delay is used. Cancelled and unobserved trains are excluded. Trains added in real time (`ADDED`, not in the GTFS) are included. | 🟡 |
| 2a | Deprogrammed trains | Deprogrammed trains / announced trains (see [train status](#train-status)). | 🟡 |
| 2b | Cancelled trains | Late-cancelled trains / announced trains. Only **full cancellations** count. A train never seen in the real-time feed is **unobserved**, not cancelled. | 🟡 |
| 2c | Removed trains | (Deprogrammed + late-cancelled trains) / announced trains. Same denominator as 2a and 2b, so 2c = 2a + 2b. | 🟡 |
| 3 | Worst lines, delay | Top 5 lines by indicator 1. Trains added in real time have no line (the real-time feed never sends `route_id`): they are grouped under an unknown line. | 🟡 |
| 4 | Worst lines, cancellations | Top 5 lines by number of trains from indicator 2b. | 🟡 |
| 5 | Problem breakdown | Trains affected per alert category. A train affected by two categories counts in both, so the chart is a **bar chart**, not a pie chart. `information_voyageur` is handled separately. | ✅ |
| 6 | Incidents | Top 5 incident reasons, by number of affected trains. | ✅ |
| 7 | Works | Top 5 lines, by number of trains affected by works. | ✅ |

Daily counts per line are stored in [`agg_daily_route`](../dbt_data_pipeline/models/warehouse/agg_daily_route.sql) ✅: only counts and sums (announced, late-cancelled, deprogrammed, added trains, delay at the effective terminus...). A week is the sum of its days, and rates are computed when reading, as a ratio of sums (e.g. indicator 2b of a week = Σ late-cancelled / Σ announced), so that days with few trains do not weigh as much as busy ones.

Weekly views are created in BigQuery by [`create_weekly_views.sql`](../bigquery/create_weekly_views.sql) ✅:

- `gold.weekly_network`: whole network per ISO week, with `days_in_week`, `is_complete_week` (7 days present) and `min_daily_observed_share` (lowest daily share of announced trains seen in real time) to spot and remove incomplete weeks.
- `gold.weekly_route`: per line and week, with the ranks of the worst-lines visuals (`delay_rank`, `late_cancelled_rank`), the values of the week before and `weeks_in_lead_*`, the number of consecutive weeks the line has been first. Trains added in real time (null line) are not ranked.

Indicators 5 to 7 come from the views `gold.weekly_alert_categories`, `gold.weekly_incident_reasons` and `gold.weekly_works_routes` ✅. They count distinct trains, which cannot be added up across categories or reasons (a train can be affected by several alerts), so they read `fact_train_alerts` directly instead of a daily aggregate. On 12/10/2026 morning: 2,773 trains affected by works, 574 by incidents and 82 by `autre`, but 3,236 distinct trains in total.

Common rules:

- A week runs from Monday to Sunday (ISO week), Europe/Paris time zone, based on the service date.
- Affected trains are counted **without duplicates**: a train affected by two alerts of the same category counts once.

### Train status

The GTFS removes trains before their service day: a train announced in one version can be missing from the next ones. Each train gets a status from the GTFS version where it disappears. ✅

| Status | Rule |
| --- | --- |
| **Planned** | In the GTFS of day D. |
| **Late-cancelled** | Removed from the GTFS less than 3 days before D (missing from the D-2, D-1 or D version while an older one had it), **or** last real-time status = `CANCELED`. |
| **Deprogrammed** | Removed from the GTFS 3 days or more before D. |

The real-time feed wins over the GTFS: a train removed from the GTFS but seen running in the real-time feed is **planned** (it ran). Trains are stored in [`fact_train_trips`](../dbt_data_pipeline/models/warehouse/fact_train_trips.sql), one row per announced or added train, with `train_status`, `is_observed`, `is_cancelled`, `is_limited` and the raw delay at the effective terminus. ✅

Trains are looked up in the **30 GTFS versions** before D. **Announced trains** = every train of day D found in at least one of these versions (planned + late-cancelled + deprogrammed): the denominator of indicators 2a, 2b and 2c.

Example for Saturday 10/10/2026: 6,385 planned + 66 late-cancelled + 2 deprogrammed = 6,453 announced trains (real-time cancellations are already among the planned trains).

Why these values, measured on 279 trains removed before their service day (service days 07/10 to 14/10/2026, versions from 15/09):

| Removed | Trains |
| --- | --- |
| 0 to 2 days before D | 162 |
| 3 to 13 days before D | 4 |
| 14 to 28 days before D, in batches over a few versions | 117 |

The two groups are clearly apart, so any cutoff between D-3 and D-13 gives the same split. Removals are seen up to 28 days ahead, the limit of the available history: hence 30 versions.

## 3. Alert classification

The standard `cause` and `effect` fields of the alerts carry almost no information:

- `effect` is always `UNKNOWN_EFFECT`.
- `cause` only takes two values: `MAINTENANCE` (reliable for works) and `OTHER_CAUSE` ("something else"). It is missing for about 12% of alerts.

Classification is therefore based on the alert **text**, using the rules table [`alert_classification_rules.csv`](../dbt_data_pipeline/seeds/alert_classification_rules.csv). ✅
The rules are applied by [`dim_alerts`](../dbt_data_pipeline/models/warehouse/dim_alerts.sql), to the last version of the day of each alert present in the feed. ✅

### How it works

1. Take the French header and description: `header || ' | ' || description`.
2. Lowercase the text and strip accents (`lower(strip_accents(...))`).
3. Each rule is a regex. A rule can also require a given `cause` value.
4. The rules whose regex matches the text are the **candidate rules**. The alert takes the candidate with the **lowest priority**.
5. When no rule matches, the alert gets the `autre` category.

### Priority order

Priorities are set in the seed, they come from no SNCF or GTFS reference. Only their order matters, and it is written in the number: the **hundreds digit is the level** of the rule, the **units its order within the level** (`303` = 3rd rule of level 3). Every priority is unique (dbt test), so no hidden tie-break decides. A new level is inserted by renumbering, never in a gap.

| Level | Content | Why at this place |
| --- | --- | --- |
| 1xx | `cause = MAINTENANCE` | Only structured signal of the feed |
| 2xx | Alert cancellation | A train that runs must not count as an incident |
| 3xx | Root causes: person struck, animal, strike, sick passenger, weather, obstacle, people on tracks, security | A cause wins over its consequence |
| 4xx | Technical failures: switch, power supply, level crossing, signalling, rolling stock, staff, train preparation | Precise consequence |
| 5xx | Generic operations: crowds, late connecting train, other infrastructure, production, traffic regulation | Vague, but better than no reason |
| 6xx | Works detected in the text | Less reliable than the `cause` field |
| 7xx | Passenger information | No impact on traffic |
| 8xx | Incident with no reason → "Non précisée" | Safety net |

Example: for "Un vol de câbles génère des anomalies de signalisation", both `surete` (3xx) and `infra_signalisation` (4xx) match. The reason kept is **Sûreté / malveillance**.

Within a level, the order of the rules was set by their `rule_id` and has not been reviewed.

### Categories

| Category | Content | Example reasons |
| --- | --- | --- |
| `incident` | Unplanned event | Matériel roulant, Régulation du trafic, Retard d'un autre train, Signalisation, Passage à niveau, Non précisée… |
| `travaux` | Planned or ongoing works | Travaux |
| `information_voyageur` | Information with no impact on traffic | Composition / quai, Capacité, Équipement / confort… |
| `autre` | No rule matches | — |

Measured on 5,413 alerts from 07/10 to 15/10/2026: incident 79.9%, passenger information 11.3%, works 8.1%, other 0.7%. A specific reason is found for 93% of incidents.

### Maintaining the rules

- Regexes use the RE2 syntax (DuckDB). They are written lowercase and without accents, except "œ", which `strip_accents` keeps.
- A growing share of `autre` means SNCF introduced a new message format: add a rule.

## 4. Linking alerts to trains

An alert lists **train numbers** and **a single active period**. It does not say on which day each number is affected. ✅ Implemented in [`fact_train_alerts`](../dbt_data_pipeline/models/warehouse/fact_train_alerts.sql), one row per alert and affected train; active periods (UTC) are converted to Paris time before checking that they overlap day D. The category and reason of the alert are not copied: they stay in [`dim_alerts`](../dbt_data_pipeline/models/warehouse/dim_alerts.sql), joined on the alert and the day.

**Rule:** a train of day D is affected by an alert when, **in the last snapshot of day D**, the alert lists its number **and** D falls within the alert's active period.

Why the version of day D: the SNCF edits the list of numbers over time, mostly removing numbers whose last train has passed. The "Weekends…" alert listed 112 numbers on 07/10, 108 on 08/10, 104 on 09/10 and 103 on 15/10. Train 426453R ran on 07/10 and was removed from the list on 08/10: with the last version of the alert, it would no longer count for 07/10.

Example with the alert "Weekends du samedi au dimanche…": it lists 112 numbers, active from 10/09 to 24/10. Over the 8 days from 07/10 to 14/10:

- 112 numbers × 8 days = 896 combinations;
- 564 match a planned train, so **564 affected trains**;
- the other 332 combinations are not trains (no service for that number that day).

Points of attention:

- `trips` and `calendar_dates` must be joined **within the same GTFS version**: `service_id` values change from one version to the next.
- An alert that disappears from the feed no longer affects the following days, even if its active period goes on (168 works alerts disappeared on 13/10).
- About 8% of the numbers listed between 07/10 and 14/10 have no train in that window. The ones checked had trains before (e.g. `OCESN426402R` in September): their last train had already passed.

## 5. Known limitations

| Limitation | Effect | Next step |
| --- | --- | --- |
| Works alerts only have a broad period (e.g. weekends only, but active from 10/09 to 24/10) | Trains affected by works are overestimated. In the example: 564 trains counted, about 90 actually affected | Ticket: extract the dates from the text |
| The real-time feed is not exhaustive | Some trains are unobserved | Track the share of unobserved trains |
| Trains added in real time (`ADDED`) have no line | Their delay (5.9% of the cumulative delay on 12/10/2026 morning) is missing from the per-line indicators | Infer the line from their stops |
| Collection gaps | On 12/10/2026, real-time snapshots stop at 11:43 (Paris time): that day's indicators are incomplete | Monitor the number of snapshots per day |

## 6. Open questions ❓

- None for now.
