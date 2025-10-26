from emulator import Job, _Context #type: ignore 
from typing import Any, Iterable, Sequence

INPUT = "origin"
OUTPUT = "output"

def clean_key(s: str) -> str:
        return s.split('_', 1)[0]

def avg_challenger_score(input_dir:str, output_dir:str):
    # DISCLAIMER: Esta tarea ya está hecha por el ejercicio 2, podría no volver a ejecutarse, pero la pusimos de vuelta acá para que cualquiera de los dos programas pueda ser ejecutado independientemente de si se ejecutó el otro previamente o no.
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
    '''
    Escribe los pares de duelos en formato id_retado - id_retador
    '''
    def fmap(key: Any, value: Any, context: _Context):
        context.write((key, int(value.split()[0])), "")

    def fred(key: Any, values: Iterable[Any], context: _Context):
        context.write(key[1], key[0])

    job = Job(input_dir, output_dir, fmap, fred)
    job.setCombiner(fred)

    job.waitForCompletion()

def join_data(input_dirs: Sequence[str], interm_dir: str, output_dir: str):

    def fsort(key_1: Any, key_2: Any):
        if len(key_1) == len(key_2):
            return 0
        elif len(key_1) < len(key_2):
            return -1
        else:
            return 1

    def fshuffle(key_1: Any, key_2: Any):
        parsed_key_1 = clean_key(key_1)
        parsed_key_2 = clean_key(key_2)
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
                context.write(v, clean_key(key) + ' ' + str(avg_challenged))

    job = Job(input_dirs[0], interm_dir, fmap_duels, fred)
    job.setShuffleCmp(fshuffle)
    job.setSortCmp(fsort)
    job.addInputPath(input_dirs[1], fmap_avg)

    job.waitForCompletion()

    def fred_final(key: Any, values: Iterable[Any], context: _Context):
        i = 0
        avg_challenger = -1
        for v in values:
            if i == 0:
                avg_challenger = v
                i += 1
            else:
                params = v.split()
                context.write(params[0], str(params[1]) + ' ' + str(clean_key(key)) + ' ' + str(avg_challenger)) #type: ignore
        

    job_2 = Job(interm_dir, output_dir, fmap_duels, fred_final)
    job_2.setShuffleCmp(fshuffle)
    job_2.setSortCmp(fsort)
    job_2.addInputPath(input_dirs[1], fmap_avg)
    job_2.waitForCompletion()


def join_heroic(input_dirs: Sequence[str], output_dir:str):
    def fsort(key_1: Any, key_2: Any):
        if len(key_1) == len(key_2):
            return 0
        elif len(key_1) < len(key_2):
            return -1
        else:
            return 1

    def fshuffle(key_1: Any, key_2: Any):
        parsed_key_1 = clean_key(key_1)
        parsed_key_2 = clean_key(key_2)
        if parsed_key_1 == parsed_key_2:
            return 0
        elif parsed_key_1 < parsed_key_2:
            return -1
        else:
            return 1
        
    def fmap_duels(key: Any, value: Any, context: _Context):
        context.write(key + '_id', value)

    def fmap_score(key: Any, value: Any, context: _Context):
        context.write(key + '_s', value)

    def fred(key: Any, values: Iterable[Any], context: _Context):
        hs_challenged = 1.0
        for v in values:
            if len(v.split()) == 1:
                hs_challenged = v
            else:
                params = v.split()
                
                context.write(params[1], params[2] + ' ' + clean_key(key) + ' ' + params[0] + ' ' + str(hs_challenged))

    job = Job(input_dirs[0], output_dir, fmap_duels, fred)
    job.addInputPath(input_dirs[1], fmap_score)
    job.setShuffleCmp(fshuffle)
    job.setSortCmp(fsort)
    job.waitForCompletion()

def update_heroic(input_dir: str, output_dir: str, alpha:float):
    def fmap(key: Any, value: Any, context: _Context):
        avg_challenger, _, avg_challenged, ph = value.split()
        result = float(ph) * float(avg_challenger) / float(avg_challenged)
        context.write(key, result)

    def fcomb(key: Any, values: Iterable[Any], context: _Context):
        total = 0
        for v in values:
            total += v
        context.write(key, total)

    def fred(key: Any, values: Iterable[Any], context: _Context):
        alpha = float(context['alpha']) #type: ignore
        total = 0
        for v in values:
            total += v
        result = alpha * total + (1 - alpha)

        context.write(key, result)

    job = Job(input_dir, output_dir, fmap, fred)
    job.setParams({'alpha': alpha})
    job.setCombiner(fcomb)

    job.waitForCompletion()

def compare_heroics(inputs:list[str], output:str) -> float:
    def fmap(key: Any, value: Any, context: _Context):
        context.write(key, float(value))

    def fred(key: Any, values: Iterable[Any], context: _Context):
        sign:int = 1
        total:float = 0
        for v in values:
            total += v*sign
            sign = -1
        total = total**2
        context.write(key, total)

    job = Job(inputs[0], 'temporal', fmap, fred)
    job.addInputPath(inputs[1], fmap)

    job.waitForCompletion()

    def fmap2(key: Any, value: Any, context: _Context):
        context.write(1, [float(value), 1])

    def fcomb(key: Any, values: Iterable[Any], context: _Context):
        total = 0
        count = 0
        for v in values:
            total += v[0]
            count += v[1]
        context.write(1, [total, count])

    def fred2(key: Any, values: Iterable[Any], context: _Context):
        total = 0
        count = 0
        for v in values:
            total += v[0]
            count += v[1]
        context.write(total/count, '')

    job = Job('temporal', output, fmap2, fred2)
    job.setCombiner(fcomb)
    job.waitForCompletion()
    with open(output + "/output.txt", "r") as f:
        value = float(f.read().strip())
    return value

def top_10(input_dir:str, output:str):
    def fsort(key_1: Any, key_2: Any):
        if key_1 == key_2:
            return 0
        elif key_1 < key_2:
            return 1
        else:
            return -1

    def fshuffle(key_1: Any, key_2: Any):
        return 0
    
    def fmap(key: Any, value: Any, context: _Context):
        context.write(float(value), [key, value])

    def fred(key: Any, values: Iterable[Any], context: _Context):
        i = 0
        for v in values:
            i += 1
            print(i)
            context.write(v[0], v[1])
            if i == 10:
                break
    
    job = Job(input_dir, output, fmap, fred)
    job.setShuffleCmp(fshuffle)
    job.setSortCmp(fsort)
    job.waitForCompletion()

avg_challenger_score('origin', 'avg_challenger_score')
intialize_heroic_score('origin', 'heroic_score')
duels_pairs('origin', 'duel_pairs')
join_data(['duel_pairs', 'avg_challenger_score'], 'duels_temporary', 'duels_with_avg_scores')

goes_to_heroic_score:bool = True

heroic_outputs:list[str] = ['heroic_score', 'secondary_heroic_score']

difference:float = 2
while difference > 0.1:
    join_heroic(['duels_with_avg_scores', heroic_outputs[int(goes_to_heroic_score)]], 'iterable')
    update_heroic('iterable', heroic_outputs[int(not goes_to_heroic_score)], 0.1)
    difference = compare_heroics(heroic_outputs, 'difference')
    goes_to_heroic_score = not goes_to_heroic_score

top_10(heroic_outputs[int(goes_to_heroic_score)], "top_10")