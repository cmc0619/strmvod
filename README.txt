[Plugin] strmvod 0.3.0
Caveats
I wanted to write it using SELECT statements on postgres.  I may still at some point.  For now it uses the Dispatcharr API with 100 item/page pulls and it's slow.  Be nice.
This whole thing is AI generated.  Hell I might be AI generated.
This will have bugs.  They'll get fixed when they annoy me enough to spend time on it.
There is no support from me
This is 100% AI written. I haven't written code in 30 years.
This will have bugs.  There is no support from me.
Be nice.  The delete thread button is a keypress away.
This will be deleted when someone with clue writes a better one.
Be nice.  Has bugs.  No support.  

All that being said if it's something easy to have ChatGPT/Claude fix I am willing to take a look (on my own schedule)


Core Features
File Generation:

Creates .strm files for movies and TV series episodes
Organizes movies into individual folders with format: Movie Name (Year)/Movie Name (Year).strm
Organizes series into: Series Name (Year)/Season ##/Series Name - S##E## - Episode Title.strm
Generates proxy URLs for Dispatcharr streaming

TMDB Integration (Optional):

Looks up proper movie and series titles from TMDB
Fetches release years and air dates
Caches TMDB lookups to minimize API calls and improve performance
Falls back to original names if TMDB unavailable or API key not provided

NFO Metadata Files (Optional):

Generates .nfo files for Jellyfin/Emby/Plex compatibility
Movie metadata: title, plot, rating, year, runtime, poster, fanart, genres
TV show metadata: tvshow.nfo with series information
Episode metadata: individual episode details with thumbnails

File Management:

Tracks all created files in a manifest
Only writes/updates files when content changes
Optional cleanup to remove stale .strm files no longer in the API
Removes empty directories after cleanup

URL Probing (Optional):

Can test URLs with HEAD requests before creating files
Skips files if URLs are unreachable

Scheduled Automation:

Background scheduler for automatic daily runs
Separate schedules for movies and series
Runs at specified times in 24-hour format (e.g., "0300,1500")
Scheduler persists across plugin reloads
Auto-restarts scheduler when settings change

Dry Run Mode:

Simulates operations without making changes
Limits processing to 1000 items for testing
Shows what would happen without touching files
Note: Scheduled runs always run for real (not dry run)

Logging & Debugging:

Verbose per-item logging option
Detailed debug logging to file (/data/vod_strm_debug.log)
Thread-safe logging works for both manual and scheduled runs
Logs appear in both Docker logs and debug file when enabled
Progress indicators every 50 movies / 20 series

Performance & Reliability:

Pagination support for large libraries
Token refresh/re-authentication on 401 errors
TMDB rate limiting (0.26s between requests)
Atomic file writes with temporary files
Threaded background scheduler

Statistics & Reporting:

Reports created, updated, skipped, and error counts
Detailed skip reasons (missing UUID, probe failed, etc.)
Shows total items processed

Configuration Options:

Dispatcharr host and credentials
Custom root directories for movies and series
Toggle all major features on/off
Flexible scheduling with multiple daily run times