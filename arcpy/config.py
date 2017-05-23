import os
import logging
import ooarcpy
import arcpy
import ec

basedir = r'C:\tsc'
debug = False
logging.basicConfig(
	level=logging.DEBUG,
	format='%(asctime)s.%(msecs)03d %(levelname)-8s %(name)s: %(message)s',
	datefmt='%Y-%m-%dT%H:%M:%S',
	filename=os.path.join(basedir, 'ec.log'),
	filemode='a'
)

log = logging.getLogger(__name__)

workspace = os.path.join(basedir, 'workspace')
workspace_cest = os.path.join(workspace, 'cest')
modeldir = os.path.join(basedir, 'model')

log.debug('basedir: %s', basedir)
log.debug('workspace: %s', workspace)
log.debug('modeldir: %s', modeldir)

if not os.path.exists(workspace):
	os.makedirs(workspace)

fgdb = ooarcpy.FileGDB(os.path.join(workspace, 'outputs.gdb'))
enviroCar = ooarcpy.FileGDB(os.path.join(basedir, 'envirocar.gdb'))
sde = ooarcpy.SDE(path = os.path.join(workspace, 'envirocar.sde'),
		  		  database = 'envirocar', hostname = 'localhost',
				  username = 'postgres',  password = 'postgres')

names = ['period1', 'period2', 'period3']
fgdbs = [ooarcpy.FileGDB(os.path.join(workspace, '%s.gdb' % name)) for name in names]

#names = ['summer', 'all'] + ['week%d' % (week+1) for week in range(8)]
#fgdbs = [ooarcpy.FileGDB(os.path.join(workspace, '%s.gdb' % name)) for name in names]

axis_model = ec.AxisModel.for_dir(modeldir)


log.debug('fgdb: %s', fgdb)


stops = fgdb.table('stops')

axes = ['2_1']
#axes = None
#axes = [a for a in ec.axis(range(1,10))]

def setenv():
	arcpy.env.overwriteOutput = True
	arcpy.env.workspace = workspace


