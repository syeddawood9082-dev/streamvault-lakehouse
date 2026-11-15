# Compaction schedule for the orders table

The orders table is merge on read. Writes land fast as delta log files and readers merge those logs with the base files at query time. That trade works while the logs stay small. Left alone the logs grow with every streaming batch and Trino reads get slower even though the row count barely moves. Compaction is the background work that merges the logs back into base files and keeps the trade honest.

Inventory does not appear in this schedule. That table is copy on write, so each write already rewrites the files it touches and there is no delta log to merge.

## The schedule

Inline compaction runs on the orders writer. After five delta commits the writer pauses long enough to compact the file groups that have pending logs. Five is the default trigger in Hudi and it fits this table because the streaming job commits in small micro batches through the day. Waiting for more commits would let a busy day stack up logs that every evening query then has to merge.

A nightly pass runs outside the writer at 02.30 UTC. It catches any file groups the inline pass skipped while the writer was busy and it is the pass to watch in the morning. The window sits after the end of day snapshot job and before the first morning queries, so a slow pass never competes with either one.

A weekly check runs on Monday morning. It reads the table timeline and counts how many file groups still carry unmerged logs. A healthy table reads zero or close to it. A count that climbs two weeks in a row means the nightly pass is not finishing and the window needs attention before query times suffer.

## Settings

The writer settings live in compaction.properties in this folder. They apply to the orders job only. The inventory job never loads them.

The settings keep inline compaction on and keep the trigger at five delta commits. They also cap how much data one compaction plan may pull in at once, so a catch up pass after a quiet weekend does not try to merge a whole week in a single plan. Splitting a big catch up across two nights is fine. Forcing it into one night is how a maintenance job turns into an outage.

## How to tell it is working

The table timeline gains compaction commits at roughly the same pace as busy ingest days. If delta commits pile up with no compaction commit between them, the schedule has stopped and the timeline shows it plainly.

Base file sizes stay in a narrow band. A file group whose base file never grows while its logs keep arriving is a group the planner keeps skipping, and that is worth a look on its own.

The daily revenue query in the sql folder keeps a steady run time across the week. That query scans recent event dates, which is exactly the read path compaction protects. A slow drift upward in its run time is usually compaction debt before it is anything else.

## When compaction falls behind

Run the nightly pass by hand once and watch it finish. Most backlogs in a table this size clear in a single extra pass.

If the backlog returns, lower the inline trigger from five delta commits to three for a week and read the timeline again at the weekly check. A lower trigger compacts more often in smaller pieces, which costs a little write speed and buys back read speed.

Never turn compaction off to make a write problem go away. The writes will feel faster for a day and every read after that pays for it. Fix the window or the trigger instead.

Never compact by hand editing files on storage. Hudi owns the timeline and the file layout. Work through the writer settings and the scheduled passes only, so a replay from Kafka can still rebuild the same table state.
