import dballe, dballe.volnd
import time
import datetime
import gc
import os
import sys
import traceback
import wxDballe.CSV
from wxDballe.Paths import DATADIR

def filePath(fileName):
	if os.access(fileName, os.R_OK):
		return fileName
	elif os.access(DATADIR + "/" + fileName, os.R_OK):
		return DATADIR + "/" + fileName
	else:
		print sys.stderr, "WARNING: file", fileName, "cannot be found either in the current directory or in", DATADIR
		return None

class DateUtils:
	fields = (
		("year", "month", "day", "hour", "min", "sec"),
		("yearmin", "monthmin", "daymin", "hourmin", "minumin", "secmin"),
		("yearmax", "monthmax", "daymax", "hourmax", "minumax", "secmax"),
		)

	EXACT = 0
	MIN = 1
	MAX = 2

def completeDate(fields, how = DateUtils.EXACT):
	"""
	Fill in the blanks from a partial date information.

	Round towards the past is how is EXACT or MIN; round towards the future
	if how is MAX.
	"""
	year, month, day, hour, minute, second = tuple(fields)

	if year == None: return None

	if how == DateUtils.MIN or how == DateUtils.EXACT:
		return datetime.datetime(year, \
				month or 1,
				day or 1,
				hour or 0,
				minute or 0,
				second or 0)
	else:
		if hour == None: hour = 23
		if minute == None: minute = 59
		if second == None: second = 59

		time = datetime.time(hour, minute, second)

		month = month or 12
		if day == None:
			# To get the day as the last day of the month,
			# get the date as "the day before the first day of the next"
			if month == 12:
				month = 1
				year = year + 1
			else:
				month = month + 1
			date = datetime.date(year, month, 1) - datetime.timedelta(1)
		else:
			date = datetime.date(year, month, day)

		return datetime.datetime.combine(date, time)

def datetimeFromRecord(rec, fieldset = DateUtils.MIN):
	fn = DateUtils.fields[fieldset]
	return completeDate([rec.enqi(fn[x]) for x in range(6)], fieldset)


class Level:
	def __init__(self, rec):
		self.type = rec.enqi("leveltype")
		self.l1 = rec.enqi("l1")
		self.l2 = rec.enqi("l2")
	def __cmp__(self, other):
		if other == None:
			return 1
		i = cmp(self.type, other.type)
		if i != 0: return i
		i = cmp(self.l1, other.l1)
		if i != 0: return i
		return cmp(self.l2, other.l2)
	def __str__(self):
		return ",".join([str(x) for x in (self.type, self.l1, self.l2)])
	def __hash__(self):
		return (self.type, self.l1, self.l2).__hash__()
	def format(self):
		return str(self.type) + ": " + dballe.describeLevel(self.type, self.l1, self.l2)

class TRange:
	def __init__(self, rec):
		self.type = rec.enqi("pindicator")
		self.p1 = rec.enqi("p1")
		self.p2 = rec.enqi("p2")
	def __cmp__(self, other):
		if other == None:
			return 1
		i = cmp(self.type, other.type)
		if i != 0: return i
		i = cmp(self.p1, other.p1)
		if i != 0: return i
		return cmp(self.p2, other.p2)
	def __str__(self):
		return ",".join([str(x) for x in (self.type, self.p1, self.p2)])
	def __hash__(self):
		return (self.type, self.p1, self.p2).__hash__()
	def format(self):
		return "%d: %s" % (self.type, dballe.describeTrange(self.type, self.p1, self.p2))

class TTracer:
	def __init__(self, name):
		self.start = time.clock()
		self.name = name
		print "  TT", self.name, ": start"
	def __del__(self):
		print "  TT", self.name, ": ", time.clock() - self.start
	def partial(self, str):
		print "  TT", self.name, "[", str, "]: ", time.clock() - self.start

class ModelListener:
	def filterDirty(self, isDirty):
		pass
	def filterChanged(self, what):
		pass
	def invalidate(self):
		pass
	def hasData(self, what):
		pass

class ProgressListener:
	def progress(self, perc, text):
		pass
	def queryError(self, message):
		pass

class UpdateInterrupted:
	pass

class Model:
	def __init__(self, dsn, user, password):
		# Init DB-ALLe and connect to the database
		self.db = dballe.DB(dsn, user, password)

		self.truncateResults = True
		self.lowerTruncateThreshold = 250
		self.resultsMax = 500
		self.resultsCount = 0
		self.resultsTruncated = False
		self.cached_results = []
		self.cached_stations = []
		self.cached_idents = []
		self.cached_dtimes = []
		self.cached_levels = []
		self.cached_tranges = []
		self.cached_vartypes = []
		self.cached_repinfo = []

		self.filter = dballe.Record()
		self.filterDirty = False
		self.activeFilter = self.filter.copy()

		self.updateListeners = []
		self.progressListeners = []

		self.updating = False
		self.updateCanceled = False

		#self.update()

	# Set the filter as dirty and notify listeners of the change
	def filterChanged(self, what = None):
		# If we are changing anything except the limits, reset the
		# limits
		if what != None and what != 'limit':
			self.resetResultLimit()

		# See if the filter has been changed
		dirty = not self.activeFilter.equals(self.filter)
		if self.filterDirty != dirty:
			for l in self.updateListeners: l.filterDirty(dirty)
			self.filterDirty = dirty
		if what != None:
			for l in self.updateListeners: l.filterChanged(what)

	def hasStation(self, s_id):
		"Return true if the given station is among the current result set"
		for id, lat, lon, ident in self.cached_stations:
			if id == s_id:
				return True
		return False

	def stations(self):
		"Generate all the station data"
		tracer = TTracer("model.stations (%d stations)" % (len(self.cached_stations)))
		for result in self.cached_stations:
			yield result

	def stationByID(self, station_id):
		"Return the data for one station, given its ID"
		for id, lat, lon, ident in self.cached_stations:
			if id == station_id:
				return id, lat, lon, ident
		return None, None, None, None

	def recordByContextAndVar(self, context, var):
		"Return a result record by context ID and variable type"
		for r in self.cached_results:
			if r.enqi("context_id") == context and r.enqc("var") == var:
				return r
		return None

	def idents(self):
		"Generate a list of idents"
		tracer = TTracer("model.idents")
		for result in self.cached_idents:
			yield result

	def hasVariable(self, context, var):
		"Returns true if the given context,var is in the current result set"
		for result in self.cached_results:
			if context == result.enqi("context_id") and var == result.enqc("var"):
				return True
		return False

	# Generate the list of all data
	def data(self):
		tracer = TTracer("model.data")
		for result in self.cached_results:
			yield result

	# Generate the list of all available datetimes
	def datetimes(self):
		tracer = TTracer("model.datetimes")
		for results in self.cached_dtimes:
			yield results
	
	# Return a 2-tuple with the minimum and maximum datetime values
	def daterange(self):
		if len(self.cached_dtimes) == 0:
			return (None, None)
		return (self.cached_dtimes[0], self.cached_dtimes[-1])

	# Generate the list of all available levels
	def levels(self):
		tracer = TTracer("model.levels")
		for val in self.cached_levels:
			yield val

	# Generate the list of all available time ranges
	def timeranges(self):
		tracer = TTracer("model.timeranges")
		for val in self.cached_tranges:
			yield val

	# Generate the list of all available variable types
	def variableTypes(self):
		tracer = TTracer("model.variableTypes")
		for val in self.cached_vartypes:
			yield val

	# Generate the list of all available reports
	def reports(self):
		tracer = TTracer("model.reports")
		for results in self.cached_repinfo:
			yield results

	# Generate the list of all attributes available for the current context
	# and varcode
	def attributes(self):
		tracer = TTracer("model.attributes")
		result = dballe.Record()
		self.db.attrQuery(self.currentContext, self.currentVarcode, result)
		for var in result:
			yield var

	def registerUpdateListener(self, func):
		self.updateListeners.append(func)

	def registerProgressListener(self, func):
		self.progressListeners.append(func)

	def checkForCancelation(self):
		if self.updateCanceled:
			for l in self.progressListeners: l.progress(100, "Canceling update...")
			raise UpdateInterrupted()

	def notifyProgress(self, perc, text):
		self.checkForCancelation()
		for l in self.progressListeners: l.progress(perc, text)
		self.checkForCancelation()

	def cancelUpdate(self):
		self.updateCanceled = True

	def queryStations(self):
		filter = self.filter.copy()
		filter.setc("query", "nosort")
		for i in ("latmin", "latmax", "lonmin", "lonmax", "ana_id", "ident", "mobile"):
			filter.unset(i)
		return self.db.queryAnaSummary(filter)

	def queryDateTimes(self):
		filter = self.filter.copy()
		filter.setc("query", "nosort")
		for i in ("yearmin", "monthmin", "daymin", "hourmin", "minumin", "secmin",
			  "yearmax", "monthmax", "daymax", "hourmax", "minumax", "secmax",
			  "year", "month", "day", "hour", "min", "sec"):
			filter.unset(i)
		return self.db.queryDateTimes(filter)

	def queryVariableTypes(self):
		filter = self.filter.copy()
		filter.setc("query", "nosort")
		filter.unset("var")
		return self.db.queryVariableTypes(filter)

	def queryReportTypes(self):
		filter = self.filter.copy()
		filter.setc("query", "nosort")
		filter.unset("rep_cod")
		filter.unset("rep_memo")
		return self.db.queryReports(filter)

	def queryLevels(self):
		filter = self.filter.copy()
		filter.setc("query", "nosort")
		filter.unset("leveltype")
		filter.unset("l1")
		filter.unset("l2")
		return self.db.queryLevels(filter)

	def queryTimeRanges(self):
		filter = self.filter.copy()
		filter.setc("query", "nosort")
		filter.unset("pindicator")
		filter.unset("p1")
		filter.unset("p2")
		return self.db.queryTimeRanges(filter)

	def writeRecord(self, record):
		(context, ana) = self.db.insert(record, True, False)

	def updateAttribute(self, context, varcode, var):
		attrs = dballe.Record()
		self.db.attrQuery(context, varcode, attrs)
		attrs.set(var)
		self.db.attrInsert(context, varcode, attrs)

	def deleteValues(self, values):
		for context, var in values:
			print "Removing", context, var
			filter = dballe.Record()
			filter.seti("context_id", context)
			filter.setc("var", var)
			self.db.remove(filter)
		self.update()

	def deleteAllAttrs(self, context, varcode):
		self.db.attrRemoveAll(context, varcode)
		self.update()

	def deleteAttrs(self, context, varcode, attrs):
		for attr in attrs:
			self.db.attrRemove(context, varcode, attr)
		self.update()

	def deleteCurrent(self):
		filter = self.activeFilter.copy()
		filter.unset("limit")
		self.db.remove(filter)
		self.update()

	def deleteOrphans(self):
		self.db.removeOrphans()
		self.update()

	def exportToFile(self, file, encoding):
		filter = self.activeFilter.copy()
		filter.unset("limit")
		if encoding == "CSV":
			exporter = wxDballe.CSV.Exporter(self.db)
			exporter.output(filter, open(file, "w"))
		elif encoding == "R":
			import dballe.rconvert, rpy
			idx = (dballe.volnd.AnaIndex(), \
			       dballe.volnd.DateTimeIndex(), \
			       dballe.volnd.LevelIndex(), \
			       dballe.volnd.TimeRangeIndex(), \
			       dballe.volnd.NetworkIndex())
			vars = dballe.volnd.read(self.db.query(filter), idx)
			dballe.rconvert.volnd_save_to_r(vars, file)
		#elif encoding == "VOLND":
		#	import dballe.volnd, cPickle
		#	idx = (dballe.volnd.AnaIndex(), \
		#	       dballe.volnd.DateTimeIndex(), \
		#	       dballe.volnd.LevelIndex(), \
		#	       dballe.volnd.TimeRangeIndex(), \
		#	       dballe.volnd.NetworkIndex())
		#	vars = dballe.volnd.read(self.db.query(filter), idx)
		#	cPickle.dump(vars, open(file, "w"))
		elif encoding == "gBUFR":
			self.db.exportResultsAsGeneric(filter, "BUFR", file)
		else:
			self.db.exportResults(filter, encoding, file)

	def update(self):
		try:
			if self.updating:
				print "ALREADY UPDATING"
				return
			self.updating = True
			self.updateCanceled = False

			self.filter.dumpToStderr()

			self.notifyProgress(0, "Beginning update.")
			#for i in 'latmin', 'latmax', 'lonmin', 'lonmax', 'ana_id':
				#print "%s: %s" % (i, self.filter.enqc(i))

			# Notify invalidation of the data in the model
			self.notifyProgress(1, "Invalidating components...")
			for l in self.updateListeners: l.invalidate()

			# Save old data so we can restore it in case the query
			# is aborted or fails
			self.oldData = {}
			self.oldData['cached_stations'] = [x for x in self.cached_stations]
			self.oldData['cached_idents'] = [x for x in self.cached_idents]
			self.oldData['cached_dtimes'] = [x for x in self.cached_dtimes]
			self.oldData['resultsCount'] = self.resultsCount
			self.oldData['resultsTruncated'] = self.resultsTruncated
			self.oldData['cached_results'] = [x for x in self.cached_results]
			self.oldData['cached_levels'] = [x for x in self.cached_levels]
			self.oldData['cached_tranges'] = [x for x in self.cached_tranges]
			self.oldData['cached_vartypes'] = [x for x in self.cached_vartypes]
			self.oldData['cached_repinfo'] = [x for x in self.cached_repinfo]
			self.oldData['active_filter'] = self.activeFilter.copy()

			self.cached_stations = []
			self.cached_idents = []
			self.cached_dtimes = []
			self.resultsCount = 0
			self.resultsTruncated = False
			self.cached_results = []
			self.cached_levels = []
			self.cached_tranges = []
			self.cached_vartypes = []
			self.cached_repinfo = []

			# Take a note of the filter that is effectively active
			self.activeFilter = self.filter.copy()

			# This is a good moment to call the garbage collector,
			# since we're throwing away lots of data and
			# regenerating it
			gc.collect()

			t = TTracer("model.update")

			# Query station data
			self.notifyProgress(2, "Querying station data...")
			idents = {}
			for result in self.queryStations():
				self.cached_stations.append((
					result.enqi("ana_id"),
					result.enqd("lat"),
					result.enqd("lon"),
					result.enqc("ident")))
				idents[result.enqc("ident")] = 1
			self.cached_idents = idents.keys()
			self.cached_idents.sort()
			t.partial("got station and ident data")

			self.notifyProgress(15, "Notifying station data...")
			for l in self.updateListeners: l.hasData("stations")
			t.partial("notified station data")
			for l in self.updateListeners: l.hasData("idents")
			t.partial("notified ident data")

			# Query datetimes
			self.notifyProgress(18, "Querying date and time data...")
			for result in self.queryDateTimes():
				self.cached_dtimes.append(
					datetime.datetime(result.enqi('year'), result.enqi('month'),
						 result.enqi('day'), result.enqi('hour'),
						 result.enqi('min'), result.enqi('sec')))
			self.cached_dtimes.sort()
			t.partial("got date and time data")
			self.notifyProgress(30, "Notifying date and time data...")
			for l in self.updateListeners: l.hasData("dtimes")
			t.partial("notified date and time data")

			# Query variable data
			self.notifyProgress(35, "Querying result data...")
			if self.truncateResults:
				# We ask for 1 more than the upper bound, to detect the
				# case where the result set has been truncated
				self.filter.seti("limit", self.resultsMax + 1)
			else:
				self.filter.seti("limit", None)
			self.filter.setc("query", "nosort")
			for result in self.db.query(self.filter):
				self.cached_results.append(result.copy())
				self.resultsCount = self.resultsCount + 1
				# Bound to the real upper bound.
				if self.truncateResults and self.resultsCount > self.resultsMax:
					self.resultsCount = self.resultsCount - 1
					self.resultsTruncated = True
					break
			t.partial("got variable data");
			self.notifyProgress(52, "Notifying result data...")
			for l in self.updateListeners: l.hasData("data")
			t.partial("notified variable data");
			self.filter.seti("limit", None)

			# Query levels
			self.notifyProgress(55, "Querying level data...")
			levels = {}
			for results in self.queryLevels():
				levels[Level(results)] = 1
			self.cached_levels = levels.keys()
			self.cached_levels.sort()
			t.partial("got level data");
			self.notifyProgress(60, "Notifying level data...")
			for l in self.updateListeners: l.hasData("levels")
			t.partial("notified level data");

			# Query time ranges
			self.notifyProgress(62, "Querying time range data...")
			tranges = {}
			for results in self.queryTimeRanges():
				tranges[TRange(results)] = 1
			self.cached_tranges = tranges.keys()
			self.cached_tranges.sort()
			t.partial("got time range data");
			self.notifyProgress(68, "Notifying time range data...")
			for l in self.updateListeners: l.hasData("tranges")
			t.partial("notified time range data");

			# Query variable types
			self.notifyProgress(70, "Querying variable types...")
			for results in self.queryVariableTypes():
				self.cached_vartypes.append(results.enqc("var"))
			t.partial("got variable type data");
			self.notifyProgress(82, "Notifying variable types...")
			for l in self.updateListeners: l.hasData("vartypes")
			t.partial("notified variable type data");

			# Query available reports
			self.notifyProgress(85, "Querying report types...")
			for results in self.queryReportTypes():
				self.cached_repinfo.append((results.enqi("rep_cod"), results.enqc("rep_memo")))
			t.partial("got repinfo data");
			self.notifyProgress(95, "Notifying report types...")
			for l in self.updateListeners: l.hasData("repinfo")
			t.partial("notified repinfo data");

			self.filter.setc("query", None)

			# Notify that all data in the model are up to date
			self.notifyProgress(98, "Notifying completion...")
			for l in self.updateListeners: l.hasData("all")
			self.notifyProgress(100, "Completed.")

			# Notify the change in filter dirtyness, without any
			# specific changes in the filter itself
			self.filterChanged()

		except UpdateInterrupted:
			print "INTERRUPTED"
			self.tidyUpAfterInterruptedQuery()
		except RuntimeError, e:
			print "QUERY FAILED", str(e)
			traceback.print_tb(sys.exc_traceback)
			self.tidyUpAfterInterruptedQuery()
			for l in self.progressListeners: l.queryError(str(e))

		self.oldData = {}
		self.updating = False

	def tidyUpAfterInterruptedQuery(self):
		# Restore the old data
		self.activeFilter = self.oldData['active_filter']

		for l in self.updateListeners: l.invalidate()
		self.cached_stations = self.oldData['cached_stations']
		self.cached_idents = self.oldData['cached_idents']
		for l in self.updateListeners: l.hasData("stations")
		for l in self.updateListeners: l.hasData("idents")
		self.cached_dtimes = self.oldData['cached_dtimes']
		for l in self.updateListeners: l.hasData("dtimes")
		self.resultsCount = self.oldData['resultsCount']
		self.resultsTruncated = self.oldData['resultsTruncated']
		self.cached_results = self.oldData['cached_results']
		for l in self.updateListeners: l.hasData("data")
		self.cached_levels = self.oldData['cached_levels']
		for l in self.updateListeners: l.hasData("levels")
		self.cached_tranges = self.oldData['cached_tranges']
		for l in self.updateListeners: l.hasData("tranges")
		self.cached_vartypes = self.oldData['cached_vartypes']
		for l in self.updateListeners: l.hasData("vartypes")
		self.cached_repinfo = self.oldData['cached_repinfo']
		for l in self.updateListeners: l.hasData("repinfo")

		for l in self.updateListeners: l.hasData("all")
		self.notifyProgress(100, "Completed.")

	def setiFilter(self, name, value):
		"""
		If the value has changed, set the value and call DirtyFilter.

		Returns True if the value has been changed, else False
		"""
		if value != self.filter.enqi(name):
			self.filter.seti(name, value)
			return True
		return False

	def setdFilter(self, name, value):
		"""
		If the value has changed, set the value and call DirtyFilter.

		Returns True if the value has been changed, else False
		"""
		if value != self.filter.enqd(name):
			self.filter.setd(name, value)
			return True
		return False

	def setcFilter(self, name, value):
		"""
		If the value has changed, set the value and call DirtyFilter.

		Returns True if the value has been changed, else False
		"""
		if value != self.filter.enqc(name):
			self.filter.setc(name, value)
			return True
		return False

	def setReportFilter(self, rep_cod):
		"""
		Set the filter to match a given report code

		Returns True if the filter has been changed, else False
		"""
		r1 = self.setiFilter("rep_cod", rep_cod);
		if r1:
			self.filterChanged("repinfo")
			return True
		return False

	def setVarFilter(self, var):
		"""
		Set the filter to match a given variable type

		Returns True if the filter has been changed, else False
		"""
		r1 = self.setcFilter("var", var)
		if r1:
			self.filterChanged("var")
			return True
		return False

	def setAnaFilter(self, filter):
		"""
		Set the ana filter expression

		Returns True if the filter has been changed, else False
		"""
		#print "ANAFilter:", filter
		r1 = self.setcFilter("ana_filter", filter)
		if r1:
			self.filterChanged("ana_filter")
			return True
		return False

	def setDataFilter(self, filter):
		"""
		Set the data filter expression

		Returns True if the filter has been changed, else False
		"""
		#print "DATAFilter:", filter
		r1 = self.setcFilter("data_filter", filter)
		if r1:
			self.filterChanged("data_filter")
			return True
		return False

	def setAttrFilter(self, filter):
		"""
		Set the attribute filter expression

		Returns True if the filter has been changed, else False
		"""
		r1 = self.setcFilter("attr_filter", filter)
		if r1:
			self.filterChanged("attr_filter")
			return True
		return False

	def setStationFilter(self, id):
		"""
		Set the filter to match a single station.

		Returns True if the filter has been changed, else False
		"""
		r1 = self.setdFilter("latmin", None)
		r2 = self.setdFilter("latmax", None)
		r3 = self.setdFilter("lonmin", None)
		r4 = self.setdFilter("lonmax", None)
		r5 = self.setdFilter("mobile", None)
		r6 = self.setcFilter("ident", None)
		r7 = self.setiFilter("ana_id", id)
		if r1 or r2 or r3 or r4 or r5 or r6 or r7:
			self.filterChanged("stations")
			return True
		return False


	def setAreaFilter(self, latmin, latmax, lonmin, lonmax):
		"""
		Set the filter to all stations in a given area.

		Returns True if the filter has been changed, else False
		"""
		r1 = self.setdFilter("latmin", latmin)
		r2 = self.setdFilter("latmax", latmax)
		r3 = self.setdFilter("lonmin", lonmin)
		r4 = self.setdFilter("lonmax", lonmax)
		r5 = self.setiFilter("ana_id", None)
		if r1 or r2 or r3 or r4 or r5:
			self.filterChanged("stations")
			return True
		return False

	def setIdentFilter(self, mobile, ident):
		"""
		Set the filter to match a given station ident.

		Returns True if the filter has been changed, else False
		"""
		r1 = self.setiFilter("mobile", mobile)
		r2 = self.setcFilter("ident", ident)
		r3 = self.setiFilter("ana_id", None)
		if r1 or r2 or r3:
			self.filterChanged("stations")
			return True
		return False

	def getLevelFilter(self, record = None):
		res = Level(record or self.filter)
		if res.type != None and res.l1 != None and res.l2 != None:
			return res;
		return None

	def setLevelFilter(self, ltype, l1, l2):
		"""
		Set the filter to match a given level.

		Returns True if the filter has been changed, else False
		"""
		r1 = self.setiFilter("leveltype", ltype)
		r2 = self.setiFilter("l1", l1)
		r3 = self.setiFilter("l2", l2)
		if r1 or r2 or r3:
			self.filterChanged("level")
			return True
		return False

	def getTimeRangeFilter(self, record = None):
		res = TRange(record or self.filter)
		if res.type != None and res.p1 != None and res.p2 != None:
			return res;
		return None

	def setTimeRangeFilter(self, pind, p1, p2):
		"""
		Set the filter to match a given time range.

		Returns True if the filter has been changed, else False
		"""
		r1 = self.setiFilter("pindicator", pind)
		r2 = self.setiFilter("p1", p1)
		r3 = self.setiFilter("p2", p2)
		if r1 or r2 or r3:
			self.filterChanged("trange")
			return True
		return False

	def getDateTimeFilter(self, filter = DateUtils.EXACT):
		return (self.filter.enqi(i) for i in DateUtils.fields[filter])

	def setDateTimeFilter(self, year, month = None, day = None, hour = None, min = None, sec = None, filter = DateUtils.EXACT):
		"""
		Set the filter to match a given minimum, maximum or exact date.

		Returns True if the filter has been changed, else False
		"""
		fn = DateUtils.fields[filter]
		r1 = self.setiFilter(fn[0], year)
		r2 = self.setiFilter(fn[1], month)
		r3 = self.setiFilter(fn[2], day)
		r4 = self.setiFilter(fn[3], hour)
		r5 = self.setiFilter(fn[4], min)
		r6 = self.setiFilter(fn[5], sec)
		if r1 or r2 or r3 or r4 or r5 or r6:
			self.filterChanged("datetime")
			return True
		return False

	def resetResultLimit(self):
		"""
		Reset the result limit to its original value.

		Returns True if the filter has been changed, else False.
		"""
		return self.setResultLimit(500)
		#if not self.truncateResults or self.resultsMax != 500:
		#	self.truncateResults = True
		#	self.resultsMax = 500
		#	changed = True
		#	self.filterChanged("limit")
		#	return True
		#return False

	def setResultLimit(self, limit):
		"""
		Set the result limit.

		Returns True if the filter has been changed, else False
		"""
		changed = False
		if limit == None and self.truncateResults:
			self.truncateResults = False;
			changed = True
		else:
			if not self.truncateResults:
				self.truncateResults = True
				changed = True
			if self.resultsMax != limit:
				self.resultsMax = limit
				changed = True
		if changed:
			self.filterChanged("limit")
			return True
		return False
