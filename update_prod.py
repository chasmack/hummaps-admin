import update
import staging
import prod
import time

if __name__ == '__main__':


    print('\nPerforming update ... ')
    startTime = time.time()

    # Create the update schema and load data from Hollins, etc.
    update.init()
    update.load_hollins()
    update.load_surveyor()
    update.load_cc()
    update.cleanup()

    # Create the staging schema and build production tables
    staging.init()

    # Transfer staging tables to the production schema
    prod.init()
    prod.load()

    endTime = time.time()
    print('{0:.3f} sec'.format(endTime - startTime))
