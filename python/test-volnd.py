#!/usr/bin/python
# -*- coding: UTF-8 -*-

import dballe
from volnd import *
import unittest, random, sys
from datetime import *
import numpy
import numpy.ma as ma

db = dballe.DB.connect_test()

class TestTddiv(unittest.TestCase):

#       def tons(td):
#               return td.days * 86400000000 + td.seconds * 1000000 + td.microseconds
	def dtest(self, td1, td2):
		self.assertEquals(tddivmod1(td1, td2), tddivmod2(td1, td2))
		q, r = tddivmod1(td1, td2)
		self.assertEquals(td2 * q + r, td1)

	def testtddiv(self):
		#self.assertEquals(tddivmod(timedelta(10, 0, 0), timedelta(2, 0, 0)), (5, timedelta(0)))
		#self.assertEquals(tddivmod(timedelta(10, 0, 1), timedelta(2, 0, 0)), (5, timedelta(0, 0, 1)))
		#self.assertEquals(tddivmod(timedelta(10, 0, 1), timedelta(3, 0, 0)), (3, timedelta(1, 0, 1)))
		#self.assertEquals(tddivmod(timedelta(10, 6, 18), timedelta(5, 3, 9)), (2, timedelta(0)))
		#self.assertEquals(tddivmod(timedelta(3, 4, 5), timedelta(1, 3, 10)), (2, timedelta(0, 86397, 999985)))
		#self.assertEquals(tddivmod(timedelta(0, 4, 5), timedelta(0, 3, 10)), (1, timedelta(0, 0, 999995)))
		#self.assertEquals(tddivmod(timedelta(2, 40, 10), timedelta(0, 0, 5)), (34568000002, timedelta(0)))

		self.dtest(timedelta(10, 0, 0), timedelta(2, 0, 0))
		self.dtest(timedelta(10, 0, 1), timedelta(2, 0, 0))
		self.dtest(timedelta(10, 0, 1), timedelta(3, 0, 0))
		self.dtest(timedelta(10, 6, 18), timedelta(5, 3, 9))
		self.dtest(timedelta(3, 4, 5), timedelta(1, 3, 10))
		self.dtest(timedelta(0, 4, 5), timedelta(0, 3, 10))
		self.dtest(timedelta(2, 40, 10), timedelta(0, 0, 5))

		# Re-enable when Debian bug #48872 has been fixed
		#self.dtest(timedelta(999999999, 86399, 999999), timedelta(0, 0, 2))

		random.seed(1)
		for i in xrange(100):
			td1 = timedelta(random.randint(0, 999999999), random.randint(0, 86400), random.randint(0, 1000000))
			td2 = timedelta(random.randint(0, 999999999), random.randint(0, 86400), random.randint(0, 1000000))
			self.dtest(td1, td2)

		# Re-enable when Debian bug #48872 has been fixed
		#for i in xrange(100):
		#       td1 = timedelta(random.randint(0, 999999999), random.randint(0, 86400), random.randint(0, 1000000))
		#       td2 = timedelta(0, random.randint(0, 86400), random.randint(0, 1000000))
		#       self.dtest(td1, td2)
		#for i in xrange(100):
		#       td1 = timedelta(random.randint(0, 999999999), random.randint(0, 86400), random.randint(0, 1000000))
		#       td2 = timedelta(0, 0, random.randint(0, 1000000))
		#       self.dtest(td1, td2)
		#for i in xrange(100):
		#       td1 = timedelta(0, random.randint(0, 86400), random.randint(0, 1000000))
		#       td2 = timedelta(0, 0, random.randint(0, 1000000))
		#       self.dtest(td1, td2)

class TestRead(unittest.TestCase):
    def setUp(self):
        # We want a predictable dataset
        random.seed(1)
        rattr = random.Random()
        rattr.seed(1)

        self.db = db

        # Wipe the test database
        self.db.reset()

        attrs = dballe.Record()
        rec = dballe.Record(mobile=0)

        def contexts():
            # 2 networks
            for net in ('synop', 'temp'):
                # 6 stations
                for lat in (10., 20., 30.):
                    for lon in (15., 25.):
                        yield net, lat, lon

        def dtrange(start, stop, delta):
            while (start < stop):
                yield start
                start += delta

        def everyxhours(x):
            return dtrange(
                    datetime(2007, 1, 1, 0, 0, 0),
                    datetime(2007, 1, 7, 0, 0, 0),
                    timedelta(0, x*3600, 0))

        def maybe_insert(rec, aname):
            if random.random() <= 0.9:
                #print repr(rec)
                ctx = self.db.insert(rec, False, True)
                del rec["context_id"]
                del rec["ana_id"]
                attrs.clear()
                attrs[aname] = rattr.random() * 100.
                for code in rec:
                    self.db.attr_insert(ctx, code, attrs)

        # Enter some sample data
        for net, lat, lon in contexts():
            rec["rep_memo"] = net
            if net == 'synop':
                aname = 'B33007'
            else:
                aname = 'B33040'
            rec["lat"] = lat
            rec["lon"] = lon

            # 6 hours precipitations
            rec["level"] = (1,)
            rec["trange"] = (4, -21600, 0)
            for dt in everyxhours(6):
                rec["date"] = dt
                rec["B13011"] = random.random()*10.
                maybe_insert(rec, aname)

            # 12 hours precipitations at different times
            rec["level"] = (1,)
            rec["trange"] = (4, -43200, 0)
            for dt in everyxhours(12):
                rec["date"] = dt
                rec["B13011"] = random.random()*10.
                maybe_insert(rec, aname)

            # Randomly measured
            # precipitations on a different
            # (meaningless) level
            # At slightly off times
            rec["level"] = (3, 2)
            rec["trange"] = (4, -21600, 0)
            for dt in everyxhours(6):
                rec["date"] = (dt + timedelta(0, random.randint(-600, 600)))
                rec["B13011"] = random.random()*10.
                maybe_insert(rec, aname)
            del rec["B13011"]

            # Pressures every 12 hours
            rec["level"] = (1,)
            rec["trange"] = (0,)
            for dt in everyxhours(12):
                rec["date"] = dt
                rec["B10004"] = float(random.randint(70000, 105000))
                maybe_insert(rec, aname)
            del rec["B10004"]

        # Insert some pseudoana data for the station 1, to test
        # pseudoana export and mixed data types
        rec.clear()
        rec.update(ana_id=1, B01001=12, B01002=123, B01019="Test of long station name", rep_cod=1)
        rec.set_station_context()
        self.db.insert(rec, False, True)

    def tearDown(self):
        #self.db.disconnect()
        pass

    def testIndexFind(self):
        # Ana in one dimension, network in the other
        query = dballe.Record(ana_id=1, var="B13011", rep_memo="synop")
        query["date"] = datetime(2007, 1, 1, 0, 0, 0)
        vars = read(self.db.query_data(query), (AnaIndex(), TimeRangeIndex()))
        self.assertEquals(vars["B13011"].dims[1].index((4, -21600, 0)), 1)

    def testFilter(self):
        # Ana in one dimension, network in the other
        query = dballe.Record(ana_id=1, var="B13011", rep_memo="synop")
        query["date"] = datetime(2007, 1, 1, 0, 0, 0)
        vars = read(self.db.query_data(query), \
                (AnaIndex(), TimeRangeIndex()), \
                filter=lambda rec: rec["timerange"] == (4, -21600, 0))
        self.assertEquals(vars["B13011"].dims[1].index((4, -21600, 0)), 0)

    def testUnsharedIndex(self):
        # Ana in one dimension, network in the other
        query = dballe.Record(ana_id=1, rep_memo="synop")

        vars = read(self.db.query_data(query),
                (AnaIndex(), TimeRangeIndex(), DateTimeIndex()))
        self.assertEquals(len(vars["B13011"].dims[2]), len(vars["B10004"].dims[2]))
        self.assertEquals(vars["B13011"].dims[2], vars["B10004"].dims[2])

        vars = read(self.db.query_data(query),
                (AnaIndex(), TimeRangeIndex(), DateTimeIndex(shared=False)))
        self.assertNotEquals(len(vars["B13011"].dims[2]), len(vars["B10004"].dims[2]))

    def testConflicts(self):
        # Ana in one dimension, network in the other
        query = dballe.Record(ana_id=1, var="B13011")
        query["date"] = datetime(2007, 1, 1, 0, 0, 0)
        # Here conflicting values are overwritten
        vars = read(self.db.query_data(query), (AnaIndex(), ), checkConflicts=False)
        self.assertEquals(type(vars), dict)
        # Here insted they should be detected
        self.assertRaises(IndexError, read,
                self.db.query_data(query),
                (AnaIndex(),),
                checkConflicts=True)

    def testFixedIndex(self):
        # Ana in one dimension, network in the other
        query = dballe.Record(ana_id=1, rep_memo="synop", year=2007, month=1, day=1)

        vars = read(self.db.query_data(query),
                (AnaIndex(), TimeRangeIndex(frozen=True,
                        start=((4, -21600, 0), (4, -43200, 0)) ) ),
                checkConflicts=False)
        self.assertEquals(len(vars["B13011"].dims[1]), 2)

        vars = read(self.db.query_data(query),
                (AnaIndex(), TimeRangeIndex()),
                checkConflicts=False)
        self.assertEquals(len(vars["B13011"].dims[1]), 3)

        vars = read(self.db.query_data(query),
                (AnaIndex(), LevelIndex(frozen=True, start=((1, None, None, None),))),
                checkConflicts=False)
        self.assertEquals(len(vars["B13011"].dims[1]), 1)

        vars = read(self.db.query_data(query),
                (AnaIndex(), LevelIndex()),
                checkConflicts=False)
        self.assertEquals(len(vars["B13011"].dims[1]), 2)

    def testAnaNetwork(self):
        # Ana in one dimension, network in the other
        query = dballe.Record()
        query["var"] = "B10004"
        query["date"] = datetime(2007, 1, 1, 0, 0, 0)
        vars = read(self.db.query_data(query), (AnaIndex(), NetworkIndex()))
        self.assertEquals(len(vars), 1)
        self.assertEquals(vars.keys(), ["B10004"])
        data = vars["B10004"]
        self.assertEquals(data.name, "B10004")
        self.assertEquals(len(data.attrs), 0)
        self.assertEquals(len(data.dims), 2)
        self.assertEquals(len(data.dims[0]), 6)
        self.assertEquals(len(data.dims[1]), 2)
        self.assertEquals(data.vals.size, 12)
        self.assertEquals(data.vals.shape, (6, 2))
        self.assertEquals(sum(data.vals.mask.flat), 1)
        self.assertEquals(ma.average(data.vals), 86890)
        self.assertEquals(data.dims[0][0], (1, 10., 15., None))
        self.assertEquals(data.dims[0][1], (2, 10., 25., None))
        self.assertEquals(data.dims[0][2], (3, 20., 15., None))
        self.assertEquals(data.dims[0][3], (4, 20., 25., None))
        self.assertEquals(data.dims[0][4], (5, 30., 15., None))
        self.assertEquals(data.dims[0][5], (6, 30., 25., None))
        self.assertEquals(set(data.dims[1]), set(((3, "temp"), (1, "synop"))))

    def testAnaTrangeNetwork(self):
        # 3 dimensions: ana, timerange, network
        # 2 variables
        query = dballe.Record(date=datetime(2007, 1, 1, 0, 0, 0))
        vars = read(self.db.query_data(query), (AnaIndex(), TimeRangeIndex(shared=False), NetworkIndex()))
        self.assertEquals(len(vars), 2)
        self.assertEquals(sorted(vars.keys()), ["B10004", "B13011"])

        data = vars["B10004"]
        self.assertEquals(data.name, "B10004")
        self.assertEquals(len(data.attrs), 0)
        self.assertEquals(len(data.dims), 3)
        self.assertEquals(len(data.dims[0]), 6)
        self.assertEquals(len(data.dims[1]), 1)
        self.assertEquals(len(data.dims[2]), 2)
        self.assertEquals(data.vals.size, 12)
        self.assertEquals(data.vals.shape, (6, 1, 2))
        self.assertEquals(sum(data.vals.mask.flat), 1)
        self.assertEquals(ma.average(data.vals), 86890)
        self.assertEquals(data.dims[0][0], (1, 10., 15., None))
        self.assertEquals(data.dims[0][1], (2, 10., 25., None))
        self.assertEquals(data.dims[0][2], (3, 20., 15., None))
        self.assertEquals(data.dims[0][3], (4, 20., 25., None))
        self.assertEquals(data.dims[0][4], (5, 30., 15., None))
        self.assertEquals(data.dims[0][5], (6, 30., 25., None))
        self.assertEquals(data.dims[1][0], (0, None, None))
        self.assertEquals(set(data.dims[2]), set(((3, "temp"), (1, "synop"))))

        data = vars["B13011"]
        self.assertEquals(data.name, "B13011")
        self.assertEquals(len(data.attrs), 0)
        self.assertEquals(len(data.dims), 3)
        self.assertEquals(len(data.dims[0]), 6)
        self.assertEquals(len(data.dims[1]), 2)
        self.assertEquals(len(data.dims[2]), 2)
        self.assertEquals(data.vals.size, 24)
        self.assertEquals(data.vals.shape, (6, 2, 2))
        self.assertEquals(sum(data.vals.mask.flat), 3)
        self.assertAlmostEquals(ma.average(data.vals), 4.033333, 6)
        self.assertEquals(data.dims[0][0], (1, 10., 15., None))
        self.assertEquals(data.dims[0][1], (2, 10., 25., None))
        self.assertEquals(data.dims[0][2], (3, 20., 15., None))
        self.assertEquals(data.dims[0][3], (4, 20., 25., None))
        self.assertEquals(data.dims[0][4], (5, 30., 15., None))
        self.assertEquals(data.dims[0][5], (6, 30., 25., None))
        self.assertEquals(data.dims[1][0], (4, -43200, 0))
        self.assertEquals(data.dims[1][1], (4, -21600, 0))
        self.assertEquals(set(data.dims[2]), set(((3, "temp"), (1, "synop"))))

        self.assertEquals(vars["B10004"].dims[0], vars["B13011"].dims[0])
        self.assertNotEquals(vars["B10004"].dims[1], vars["B13011"].dims[1])
        self.assertEquals(vars["B10004"].dims[2], vars["B13011"].dims[2])

    def testAttrs(self):
        # Same export as testAnaNetwork, but check that the
        # attributes are synchronised
        query = dballe.Record()
        query["var"] = "B10004"
        query["date"] = datetime(2007, 1, 1, 0, 0, 0)
        vars = read(self.db.query_data(query), (AnaIndex(), NetworkIndex()), attributes=True)
        self.assertEquals(len(vars), 1)
        self.assertEquals(vars.keys(), ["B10004"])
        data = vars["B10004"]
        self.assertEquals(len(data.attrs), 2)
        self.assertEquals(sorted(data.attrs.keys()), ['B33007', 'B33040'])

        for net, a in ('synop', 'B33007'), ('temp', 'B33040'):
            self.assertEquals(data.dims, data.attrs[a].dims)
            self.assertEquals(data.vals.size, data.attrs[a].vals.size)
            self.assertEquals(data.vals.shape, data.attrs[a].vals.shape)

            # Find what is the network dimension where we have the attributes
            netidx = -1
            for idx, n in enumerate(data.dims[1]):
                    if n[1] == net:
                            netidx = idx
                            break
            self.assertNotEquals(netidx, -1)

            # No attrs in the other network
            self.assertEquals([x for x in data.attrs[a].vals.mask[:,1-netidx].flat], [True]*len(data.attrs[a].vals.mask[:,1-netidx].flat))
            # Same attrs as values in this network
            self.assertEquals([x for x in data.vals.mask[:,netidx].flat], [x for x in data.attrs[a].vals.mask[:,netidx].flat])
        self.assertEquals(ma.average(data.attrs['B33007'].vals), 53.5)
        self.assertEquals(ma.average(data.attrs['B33040'].vals), 36.8)

    def testSomeAttrs(self):
        # Same export as testAnaNetwork, but check that the
        # attributes are synchronised
        query = dballe.Record()
        query["var"] = "B10004"
        query["date"] = datetime(2007, 1, 1, 0, 0, 0)
        vars = read(self.db.query_data(query), (AnaIndex(), NetworkIndex()), attributes=('B33040',))
        self.assertEquals(len(vars), 1)
        self.assertEquals(vars.keys(), ["B10004"])
        data = vars["B10004"]
        self.assertEquals(len(data.attrs), 1)
        self.assertEquals(data.attrs.keys(), ['B33040'])

        a = data.attrs['B33040']
        self.assertEquals(data.dims, a.dims)
        self.assertEquals(data.vals.size, a.vals.size)
        self.assertEquals(data.vals.shape, a.vals.shape)

        # Find the temp index
        netidx = -1
        for idx, n in enumerate(data.dims[1]):
                if n[1] == "temp":
                        netidx = idx
                        break
        self.assertNotEquals(netidx, -1)

        # Only compare the values on the temp index
        self.assertEquals([x for x in a.vals.mask[:,1-netidx].flat], [True]*len(a.vals.mask[:,1-netidx].flat))
        self.assertEquals([x for x in data.vals.mask[:,netidx].flat], [x for x in a.vals.mask[:,netidx].flat])
        self.assertEquals(ma.average(a.vals), 36.8)

    def testEmptyExport(self):
        query = dballe.Record()
        query["ana_id"] = 5000
        vars = read(self.db.query_data(query), (AnaIndex(), NetworkIndex()), attributes=True)
        self.assertEquals(len(vars), 0)

    def testGhostIndexes(self):
        # If an index rejects a variable after another index
        # has successfuly added an item, we used to end up with
        # a 'ghost' index entry with no items in it
        indexes = (TimeRangeIndex(), \
                  LevelIndex(frozen=True, start=((3, 2, None, None),) ))
        query = dballe.Record()
        query['ana_id'] = 1
        query['var'] = 'B13011'
        vars = read(self.db.query_data(query), indexes, \
                        checkConflicts=False)
        self.assertEquals(vars.keys(), ["B13011"])
        self.assertEquals(len(vars["B13011"].dims[1]), 1)
        self.assertEquals(vars["B13011"].dims[0][0], (4, -21600, 0))

    def testBuggyExport1(self):
        indexes = (AnaIndex(),
                LevelIndex(frozen=True, start=((1, None, None), (3, 2, None))),
                TimeRangeIndex(),
                DateTimeIndex())
        query = dballe.Record()
        query['rep_memo'] = 'synop'
        vars = read(self.db.query_data(query), indexes,
                checkConflicts=True, attributes=True)

    def testExportAna(self):
        indexes = (AnaIndex(),)
        query = dballe.Record()
        query.set_station_context()
        query["rep_cod"] = 1
        vars = read(self.db.query_data(query), indexes, checkConflicts=True)
        self.assertEquals(sorted(vars.keys()), ["B01001", "B01002", "B01019"])

    def testExportSyncAna(self):
        # Export some data
        indexes = (AnaIndex(), DateTimeIndex())
        query = dballe.Record()
        query["rep_memo"] = 'synop'
        query["level"] = (1,)
        query["trange"] = (4, -21600, 0)
        vars = read(self.db.query_data(query), indexes, checkConflicts=True)
        self.assertEquals(sorted(vars.keys()), ["B13011"])

        # Freeze all the indexes
        for i in range(len(indexes)):
            indexes[i].freeze()

        # Export the pseudoana data in sync with the data
        query.clear()
        query.set_station_context()
        query["rep_cod"] = 1
        anas = read(self.db.query_data(query), (indexes[0],), checkConflicts=True)

        self.assertEquals(sorted(anas.keys()), ["B01001", "B01002", "B01019"])
        self.assertEquals(anas["B01001"].dims[0], vars["B13011"].dims[0])

if __name__ == "__main__":
    db.connect_test();
    unittest.main()

# This is already automatically done
#if len(sys.argv) == 1:
#	unittest.main()
#else:
#	suite = unittest.TestLoader().loadTestsFromNames(
#			map(lambda x: __name__ + '.' + x, sys.argv[1:]))
#	unittest.TextTestRunner().run(suite)



#query = dballe.Record()
##query.set("var", "B12001")
##vars = readv7d(db.query(query), (AnaIndex,NetworkIndex))
##vars = readv7d(db.query(query), (AnaIndex,LevelIndex))
##vars = read(db.query(query), (AnaIndex(),DateTimeIndex()))
#
#query.set("rep_memo", "temp")
#vars = read(db.query(query), (AnaIndex(),IntervalIndex(datetime(2007,01,11,11,24), timedelta(0, 120), timedelta(0, 60))))
#
#print vars
##print vars['B12001'].dims[0][:20]
##print vars['B12001'].dims[1][:20]
#
##print "Lat:", res[0][:10]
##print "Lon:", res[1][:10]
##print "Id :", res[2][:10]
