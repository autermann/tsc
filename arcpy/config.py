import os
import logging

from arcpy import env
from ooarcpy import FileGDB, SDE
from ec import axis, AxisModel

logging.basicConfig(
	level=logging.DEBUG,
	format='%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s',
	datefmt='%Y-%m-%dT%H:%M:%S',
	filename=r'C:\tsc\ec.log',
	filemode='w' #overwrite log file
)

log = logging.getLogger(__name__)



basedir = r'C:\tsc'
workspace = os.path.join(basedir, 'workspace')
modeldir = os.path.join(basedir, 'model')

log.debug('basedir: %s', basedir)
log.debug('workspace: %s', workspace)
log.debug('modeldir: %s', modeldir)

if not os.path.exists(workspace):
	os.makedirs(workspace)

sde = SDE(path = os.path.join(workspace, 'envirocar.sde'),
          database = 'envirocar', hostname = 'localhost',
          username = 'postgres',  password = 'postgres')

axis_model = AxisModel.for_dir(modeldir)

fgdb = FileGDB(os.path.join(workspace, 'outputs.gdb'))

log.debug('fgdb: %s', fgdb)


stops = fgdb.table('stops')



#axes = None
#axes = [axis for axis in axis(xrange(3, 4 + 1))]
axes = ['4_2']
#axes = ['1_1','1_2']


def setenv():
	env.overwriteOutput = True
	env.workspace = workspace

debug = True