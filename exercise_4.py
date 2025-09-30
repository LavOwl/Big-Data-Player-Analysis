from emulator import Job, _Context #type: ignore 
from typing import Any, Iterable, Sequence

INPUT = "origin"
OUTPUT = "output"

def avg_challenger_score(input_dir:str, output_dir:str):
    def fmap(key: Any, value: Any, context: _Context):
        context.write(key, (int(value.split()[1]), 1))
        context.write(value.split()[0], (0, 0))

    def fcomb(key: Any, values: Iterable[Any], context: _Context):
        total = 0
        cant = 0
        for v in values:
            total += v[0]
            cant += v[1]
        context.write(key, (total, cant))

    def fred(key: Any, values: Iterable[Any], context: _Context):
        total = 0
        cant = 0
        for v in values:
            total += v[0]
            cant += v[1]
        context.write(key, (total+1)/(cant+1))

    job = Job(input_dir, output_dir, fmap, fred)
    job.setCombiner(fcomb)
    job.waitForCompletion()

def intialize_heroic_score(input_dir:str, output_dir:str):
    def fmap(key: Any, value: Any, context: _Context):
        context.write(key, 1)
        context.write(value.split()[0], 1)

    def fred(key: Any, values: Iterable[Any], context: _Context):
        context.write(key, 1)

    job = Job(input_dir, output_dir, fmap, fred)
    job.setCombiner(fred)

    job.waitForCompletion()

def duels_pairs(input_dir:str, output_dir:str):
    def fmap(key: Any, value: Any, context: _Context):
        context.write((key, int(value.split()[0])), "")

    def fred(key: Any, values: Iterable[Any], context: _Context):
        context.write(key[1], key[0])

    job = Job(input_dir, output_dir, fmap, fred)
    job.setCombiner(fred)

    job.waitForCompletion()

def join_data(input_dirs: Sequence[str], interm_dir: str, output_dir: str):

    def extract_number(s: str) -> str:
        return s.split('_', 1)[0]

    def fsort(key_1: Any, key_2: Any):
        if len(key_1) == len(key_2):
            return 0
        elif len(key_1) < len(key_2):
            return -1
        else:
            return 1

    def fshuffle(key_1: Any, key_2: Any):
        parsed_key_1 = extract_number(key_1)
        parsed_key_2 = extract_number(key_2)
        if parsed_key_1 == parsed_key_2:
            return 0
        elif parsed_key_1 < parsed_key_2:
            return -1
        else:
            return 1

    def fmap_duels(key: Any, value: Any, context: _Context):
        context.write(key + '_id', value)

    def fmap_avg(key: Any, value: Any, context: _Context):
        context.write(key + '_s', value)

    def fred(key: Any, values: Iterable[Any], context: _Context):
        i = 0
        avg_challenged = -1
        for v in values:
            if i == 0:
                avg_challenged = v
                i += 1
            else:
                context.write(v, (extract_number(key), avg_challenged))

    job = Job(input_dirs[0], interm_dir, fmap_duels, fred)
    job.setShuffleCmp(fshuffle)
    job.setSortCmp(fsort)
    job.addInputPath(input_dirs[1], fmap_avg)

    job.waitForCompletion()

    def fred_final(key: Any, values: Iterable[Any], context: _Context):
        i = 0
        avg_challenger = -1
        params = ""
        for v in values:
            if i == 0:
                avg_challenger = v
                i += 1
            else:
                params = v.strip("()").replace("'", "").split(", ")
                context.write(params[0], (params[1], extract_number(key), avg_challenger)) #type: ignore
        

    job_2 = Job(interm_dir, output_dir, fmap_duels, fred_final)
    job_2.setShuffleCmp(fshuffle)
    job_2.setSortCmp(fsort)
    job_2.addInputPath(input_dirs[1], fmap_avg)
    job_2.waitForCompletion()


def join_heroic(input_dirs: Sequence[str], output_dir:str):
    pass

def update_heroic(input_dir: str, output_dir: str):
    def fmap(key: Any, value: Any, context: _Context):
        alpha = float(context['alpha']) #type: ignore
        _, ph, avg_challenged, avg_challenger = value.split()
        context.write(key, alpha*ph*avg_challenger/avg_challenged)

    def fred(key: Any, values: Iterable[Any], context: _Context):
        alpha = float(context['alpha']) #type: ignore
        total = 0
        cant = 0
        for v in values:
            total += v
            cant += 1
        result = (total/cant) + 1 - alpha

        context.write(key, result)

    job = Job(input_dir, output_dir, fmap, fred)
    #job.setCombiner(fred) HACER COMBINER DSP, SE PUEDE

    job.waitForCompletion()

avg_challenger_score('origin', 'avg_challenger_score')
intialize_heroic_score('origin', 'heroic_score')
duels_pairs('origin', 'duel_pairs')
join_data(['duel_pairs', 'avg_challenger_score'], 'duels_temporary', 'duels_with_avg_scores')