from emulator import Job
from emulator import _Context #type: ignore
from typing import Any, Iterable

ORIGIN = "origin"
CHALLENGES_PER_CHALLENGER = "challenges_per_challenger"
OUTPUT = "output"

def aggregate_by_challenger(input_dir:str, output_dir:str) -> None:
    """sumary_line
    Writes to OUTPUT directory the tuples (challenger_id, amount_of_challenges) reading from INPUT directory the weekly challenges (challenger_id, challenged_id, score, duration). 
    Keyword arguments:
    input_dir -- directory from where the challenges will be read.
    output_dir -- directory where the amount of challenges will be written.
    Return: None.
    """
    
    def fmap(key: Any, value: Any, context: _Context):
        params = value.split()
        context.write(key, (1, int(params[1])))

    def fcomb(key: Any, values: Iterable[Any], context: _Context):
        total = 0
        score = 0
        for v in values:
            total += v[0]
            score += v[1]
        context.write(key, (total, score))

    def fred(key: Any, values: Iterable[Any], context: _Context):
        total = 0
        score = 0
        for v in values:
            total += v[0]
            score += v[1]
        context.write(key, (score+1)/(total+1))

    job = Job(input_dir, output_dir, fmap, fred)
    job.setCombiner(fcomb)
    job.waitForCompletion()

def calculate_maximum(input_dir:str, output_dir:str) -> None:
    """sumary_line
    Writes to OUTPUT directory the tuple (challenger_id, 1), reading from INPUT directory the amount of challenges per challenger (challenger_id, amount_of_challenges). 
    Keyword arguments:
    input_dir -- directory from where the challenges will be read.
    output_dir -- directory where the amount of challenges will be written.
    Return: None.
    """
    
    def fmap(key: Any, value: Any, context: _Context):
        context.write(1, (key, float(value)))

    def fcomb(key: Any, values: Any, context: _Context):
        max_challenges = -1
        max_challenger = -1
        print()
        for v in values:
            if v[1] > max_challenges:
                max_challenges = v[1]
                max_challenger = v[0]
        context.write(1, (max_challenger, max_challenges))

    def fred(key: Any, values: Iterable[Any], context: _Context):
        max_challenges = -1
        max_challenger = -1
        for v in values:
            if v[1] > max_challenges:
                max_challenges = v[1]
                max_challenger = v[0]
        context.write(max_challenger, 1)

    job = Job(input_dir, output_dir, fmap, fred)
    job.setCombiner(fcomb)
    job.waitForCompletion()

aggregate_by_challenger(ORIGIN, CHALLENGES_PER_CHALLENGER)
calculate_maximum(CHALLENGES_PER_CHALLENGER, OUTPUT)