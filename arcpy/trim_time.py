from config import sde, setenv

setenv()

if __name__ == '__main__':
  #l = len('YYYY-MM-DD HH:MM:SS')
  fc = sde.feature_class('measurements')
  with fc.update(['time']) as cursor:
    for row in cursor:
      row[0] = row[0].replace(microsecond=0)
      #.strftime('%Y-%m-%d %H:%M:%S')
      cursor.updateRow(row)