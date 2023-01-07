import logging
import argparse
import pandas as pd
from tqdm import tqdm
from pytube import Channel, Playlist
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import TextFormatter, JSONFormatter
from concurrent.futures import ThreadPoolExecutor


def download_video(video, visual_flag, audio_flag, transcript_flag):
    video_id = video.video_id
    streams = video.streams

    if visual_flag:
        streams.get_by_itag("244").download(
            output_path="data/video",
            filename=f"video_{video_id}.mp4"
        )

    if audio_flag:
        streams.get_by_itag("139").download(
            output_path="data/audio",
            filename=f"audio_{video_id}.mp4"
        )

    transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=["en"])
    transcript_text = TextFormatter().format_transcript(transcript)

    if transcript_flag:
        transcript_json = JSONFormatter().format_transcript(transcript)
        with open(f"data/caption/captions_{video_id}.json", "w", encoding="utf-8") as outfile:
            outfile.write(transcript_json)

    stats = {
        "id": video_id,
        "date": video.publish_date.strftime("%Y-%m-%d"),
        "fps": "30fps",
        "res": "480p",
        "abr": "48kbps",
        "title": video.title,
        "transcript": transcript_text,
        "length": video.length,
        "views": video.views,
    }

    return stats


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="{asctime} {levelname:<8} {message}",
        style="{",
        filename=f"saved/logs/{__file__[:-3]}.log",
        filemode="w"
    )

    logging.warning("This is a warning level msg")
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume", action="store_const", const=True)
    parser.add_argument("-v", "--visual", action="store_const", const=True, default=True)
    parser.add_argument("-a", "--audio", action="store_const", const=True, default=True)
    parser.add_argument("-t", "--transcript", action="store_const", const=True, default=True)
    parser.add_argument("--playlist")
    args = parser.parse_args()

    if args.resume:
        ids = set(pd.read_csv("data/moist_stats.csv")["id"])

    if args.playlist:
        video_collection = Playlist(args.playlist).videos
    else:
        video_collection = Channel('https://www.youtube.com/user/penguinz0').videos

    pool = ThreadPoolExecutor(max_workers=5)
    df = []

    for video in tqdm(video_collection):
        if video.length / 60 > 30:
            continue
        if args.resume and video.video_id in ids:
            continue
        if not video.title.startswith("Moist Meter"):
            continue

        try:
            future = pool.submit(download_video,
                                 video,
                                 args.visual,
                                 args.audio,
                                 args.transcript)

            df.append(future.result())
        except Exception as e:
            logging.warning(f"Error at {video.video_id}")
            logging.warning(e)

    pool.shutdown()

    df = pd.DataFrame(df)
    if args.resume:
        df.to_csv("data/moist_stats.csv", mode="a", index=False, header=False)
    else:
        df.to_csv("data/moist_stats.csv", index=False)
