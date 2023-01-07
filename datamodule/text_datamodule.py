from collections import namedtuple
from functools import reduce
from dataclasses import dataclass, field
from typing import List, Callable


VideoMetadata = namedtuple(
    "VideoMetadata",
    ("id", "date", "fps", "res", "abr", "title", "transcript", "length", "views")
)


@dataclass
class MoistItem:
    prompt: str = field(init=False)
    completion: str = field(init=False)

    video_metadata: "VideoMetadata" = field(repr=False)
    transforms: List[Callable] = field(repr=False)

    def __post_init__(self):
        self.prompt, self.completion = reduce(
            lambda x, func: func(x),
            self.transforms,
            self.video_metadata
        )

    @staticmethod
    def clean_text(text: str) -> str:
        text = text.replace("[\xa0__\xa0]", "")
        text = text.replace("\n", " ")

        return text

    @staticmethod
    def format_moist_meter(video_metadata: "VideoMetadata") -> tuple:
        identity = "I am MoistCr1TiKaL."
        title = video_metadata.title.replace("Moist Meter | ", "")
        instructions = (
            "Write a detailed, exaggerated, satirical review of "
            f"'{title}' using a combination of similes, hyperboles, and "
            "pop culture references"
        )

        prompt = "\n\n".join([identity, instructions])
        completion = MoistItem.clean_text(video_metadata.transcript)

        return prompt, completion
