from emulator import Job, _Context #type: ignore 
from typing import Any, Iterable

ORIGIN = "origin"
INTERM = "interm"
OUTPUT = "output"
H_VAL = 12


def remove_duplicates(input_dir:str, output_dir:str):
    def fmap(key: Any, value: Any, context: _Context):
        params = value.split()
        context.write((key, params[0]), 1)

    def fred(key: Any, values: Iterable[Any], context: _Context):
        context.write(key[0], key[1])

    job = Job(input_dir, output_dir, fmap, fred)

    job.setCombiner(fred)

    job.waitForCompletion()


def above_H(input_dir:str, output_dir:str, h: int):
    def fmap(key: Any, value: Any, context: _Context):
        context.write(key, 1) #type: ignore

    def fcomb(key: Any, values: Iterable[Any], context: _Context):
        challengers = 0
        for v in values:
            challengers += v
        context.write(key, challengers)

    def fred(key: Any, values: Iterable[Any], context: _Context):
        challengers = 0
        for v in values:
            challengers += v
        if challengers >= int(context['requisite']): #type: ignore
            context.write(key, challengers)

    job = Job(input_dir, output_dir, fmap, fred)

    job.setCombiner(fcomb)
    job.setParams({'requisite': H_VAL})

    job.waitForCompletion()


remove_duplicates(ORIGIN, INTERM)
above_H(INTERM, OUTPUT, H_VAL)