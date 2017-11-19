import time

SCHEMA_PROD = 'hummaps'


def init():
    # Create the production schema

    print('CREATE SCHEMA: {schema} ...'.format(schema=SCHEMA_PROD))


def load():
    # Copy tables from staging to production

    pass


if __name__ == '__main__':


    print('\nPerforming update ... ')
    startTime = time.time()

    # Create the production schema and copy tables from staging.
    init()
    load()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))
