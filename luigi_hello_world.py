""" Toy example of hello world pipeline with luigi """
import luigi
import os
from time import sleep

SHORT_SLEEP = 5
LONG_SLEEP = 10


class MakeDirectory(luigi.Task):
    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.path)

    def run(self):
        os.makedirs(self.path)


class WorldTask(luigi.Task):
    path = luigi.Parameter()

    def run(self):
        sleep(SHORT_SLEEP)
        with open(self.path, "w") as world_file:
            world_file.write("World")
            world_file.close()

    def output(self):
        return luigi.LocalTarget(self.path)

    def requires(self):
        return [
            MakeDirectory(path=os.path.dirname(self.path)),
        ]


class HelloTask(luigi.Task):
    path = luigi.Parameter()

    def run(self):
        sleep(LONG_SLEEP)
        with open(self.path, "w") as hello_file:
            hello_file.write("Hello")
            hello_file.close()

    def output(self):
        return luigi.LocalTarget(self.path)

    def requires(self):
        return [
            MakeDirectory(path=os.path.dirname(self.path)),
        ]


class PrintWordTask(luigi.Task):
    path = luigi.Parameter()
    word = luigi.Parameter()

    def requires(self):
        return [
            MakeDirectory(path=os.path.dirname(self.path)),
        ]

    def output(self):
        return luigi.LocalTarget(self.path)

    def run(self):
        sleep(SHORT_SLEEP)
        with open(self.path, "w") as out_file:
            out_file.write(self.word)
            out_file.close()


class HelloWorldTask(luigi.Task):
    id = luigi.Parameter(default="test")

    def run(self):
        sleep(LONG_SLEEP)
        with open(self.input()[0].path, "r") as hello_file:
            hello = hello_file.read()
        with open(self.input()[1].path, "r") as world_file:
            world = world_file.read()
        with open(self.output().path, "w") as output_file:
            content = "{} {}!".format(hello, world)
            output_file.write(content)
            output_file.close()

    def requires(self):
        return [
            PrintWordTask(path="results/{}/hello.txt".format(self.id), word="Hello"),
            PrintWordTask(path="results/{}/world.txt".format(self.id), word="World"),
        ]

    def output(self):
        path = "results/{}/hello_world.txt".format(self.id)
        return luigi.LocalTarget(path)


if __name__ == "__main__":
    luigi.run()
