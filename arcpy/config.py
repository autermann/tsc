import os
import logging

from arcpy import env
from ooarcpy import FileGDB, SDE
from ec import axis, AxisModel

logging.basicConfig(
	level=logging.DEBUG,
	format='%(asctime)s.%(msecs)03d %(levelname)-8s %(name)s: %(message)s',
	datefmt='%Y-%m-%dT%H:%M:%S',
	filename=r'C:\tsc\ec.log',
	filemode='a'
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

#sde = SDE(path = os.path.join(workspace, 'envirocar.sde'),
#          database = 'envirocar', hostname = 'localhost',
#          username = 'postgres',  password = 'postgres')

sde = FileGDB(os.path.join(basedir, 'envirocar.gdb'))

axis_model = AxisModel.for_dir(modeldir)

fgdb = FileGDB(os.path.join(workspace, 'outputs.gdb'))

log.debug('fgdb: %s', fgdb)


stops = fgdb.table('stops')


axes = None
#axes = ['12_2']
#axes = [x for x in axis(xrange(16, 20))] + [x for x in axis(xrange(1, 10))]
#axes = ['4_2']
#axes = ['1_1','1_2']


def setenv():
	env.overwriteOutput = True
	env.workspace = workspace

debug = True
