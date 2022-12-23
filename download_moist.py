import argparse
import pandas as pd
from tqdm import tqdm
from pytube import Channel
from concurrent.futures import ThreadPoolExecutor


def download_video(video):
    video_id = video.video_id
    streams = video.streams

    streams.get_by_itag("244").download(
        output_path="data/video",
        filename=f"video_{video_id}.mp4"
    )

    streams.get_by_itag("139").download(
        output_path="data/audio",
        filename=f"audio_{video_id}.mp4"
    )

    if video.captions.get("en"):
        captions = video.captions["en"]
    elif video.captions.get("a.en"):
        captions = video.captions["a.en"]
    else:
        captions = None

    if captions is not None:
        with open(f"data/caption/captions_{video_id}.xml", "w") as f:
            f.write(captions.xml_captions)

    stats = {
        "id": video_id,
        "date": video.publish_date.strftime("%Y-%m-%d"),
        "fps": "30fps",
        "res": "480p",
        "abr": "48kbps",
        "title": video.title,
        "length": video.length,
        "views": video.views,
        "captions": captions is not None
    }

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume", action="store_const", const=True)
    args = parser.parse_args()

    if args.resume:
        ids = set(pd.read_csv("data/moist_stats.csv")["id"])

    pool = ThreadPoolExecutor(max_workers=5)
    df = []
    moist_channel = Channel('https://www.youtube.com/user/penguinz0')
    for video in tqdm(moist_channel.videos):
        if video.length / 60 > 30:
            continue
        if args.resume and video.video_id in ids:
            continue

        try:
            future = pool.submit(download_video, video)
            df.append(future.result())
        except KeyError as e:
            print(f"{e}...skipping {video.title}")
        except Exception as e:
            print(f"{e}...stopping at video {video.title}")
            break

    pool.shutdown()

    df = pd.DataFrame(df)
    if args.resume:
        df.to_csv("data/moist_stats.csv", mode="a", index=False, header=False)
    else:
        df.to_csv("data/moist_stats.csv", index=False)
