cp ../playindex/formatted_videos.csv formatted_videos.csv
python shot_collect.py
git add --all
git commit -m 'daily'
git push 