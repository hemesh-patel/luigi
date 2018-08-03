import luigi
import os


class MakeDirectory(luigi.Task):

    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.path)

    def run(self):
        os.makedirs(self.path)

class PrintWordTask(luigi.Task):

    path = luigi.Parameter()
    word = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.path)

    def run(self):
        with open(self.path, 'w') as out_file:
            out_file.write(self.word)
            out_file.close()

    def requires(self):
        return[
            [MakeDirectory(path=os.path.dirname(self.path))]  # This creates the directory in which the file will sit
        ]

"""
class HelloTask(luigi.Task):

    path = luigi.Parameter()  # We define a parameter that this class needs

    def run(self):
        with open(self.path, 'w') as hello_file:  # The actual path is defined in the HelloWorldTask class
            hello_file.write('Hello')
            hello_file.close()

    def output(self):
        # This checks to see if the file exits. If it exists the 'run' function is not run
        return luigi.LocalTarget(self.path)

    def requires(self):
        return [MakeDirectory(path=os.path.dirname(self.path))]  # This creates the directory in which the file will sit


class WorldTask(luigi.Task):

    path = luigi.Parameter()  # We define a parameter that this class needs

    def run(self):
        with open(self.path, 'w') as world_file:  # The actual path is defined in the HelloWorldTask class
            world_file.write('World')
            world_file.close()

    def output(self):
        return luigi.LocalTarget(self.path)

    def requires(self):
        return [MakeDirectory(path=os.path.dirname(self.path))]

"""

class HelloWorldTask(luigi.Task):

    id = luigi.Parameter(default='test')

    def run(self):
        # The self.input() reads the 'requires' function - note it is a list
        with open(self.input()[0].path, 'r') as hello_file:
            hello = hello_file.read()
        with open(self.input()[1].path, 'r') as world_file:
            world = world_file.read()
        # We collect the out of the output from the 'output' function and use it here
        with open(self.output().path, 'w') as output_file:
            content = '{} {}'.format(hello, world)
            output_file.write(content)
            output_file.close()

    def output(self):
        path = 'results/{}/hello_world.txt'.format(self.id)  # Note the results directory must be created
        return luigi.LocalTarget(path)

    def requires(self):
        # Here we create dependencies
        # The classes listed here need to be completed before 'run' function in this class is run
        return[
            ##HelloTask(path='results/{}/hello.txt'.format(self.id)),
            #WorldTask(path='results/{}/world.txt'.format(self.id)),
            PrintWordTask(path='results/{}/hello.txt'.format(self.id), word='Hello'),
            PrintWordTask(path='results/{}/world.txt'.format(self.id), word='World'),

        ]

if __name__ == '__main__':
    # Run the pipeline by running the python code - note command line
    luigi.build([HelloWorldTask(id='gandalf'), ], workers=1, local_scheduler=True)
    # Note we should typically run a single task and that task should have dependencies.
