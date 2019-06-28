#!/usr/bin/env python3
import argparse
import sys
import logging

import dballe


logger = logging.getLogger(__name__)


def pass_qc(attrs):
    attrs_dict = {
        v.code: v.get() for v in attrs
    }

    if attrs_dict.get("B33007", 100) == 0:
        return False

    if attrs_dict.get("B33192", 100) == 0:
        return False

    if attrs_dict.get("B33196", 100) == 1:
        return False

    total_score = 0

    for bcode, threshold, lt_score, gte_score in (
        ("B33192", 10, -1, 0),
        ("B33193", 10, -1, 1),
        ("B33194", 10, -1, 1),
    ):
        if bcode in attrs_dict:
            if attrs_dict[bcode] < threshold:
                total_score = total_score + lt_score
            else:
                total_score = total_score + gte_score

    return total_score >= -1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--preserve", action="store_true")
    parser.add_argument("inputfiles", nargs="*", metavar="FILE",
                        help="BUFR file")

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG,
                        handlers=[logging.StreamHandler()])

    if not args.inputfiles:
        inputfiles = [sys.stdin]
    else:
        inputfiles = args.inputfiles

    importer = dballe.Importer("BUFR")
    exporter = dballe.Exporter("BUFR")

    for f in inputfiles:
        with importer.from_file(f) as fp:
            for msgs in fp:
                for msg in msgs:
                    new_msg = dballe.Message("generic")
                    new_msg.set_named("year", dballe.var("B04001", msg.datetime.year))
                    new_msg.set_named("month", dballe.var("B04002", msg.datetime.month))
                    new_msg.set_named("day", dballe.var("B04003", msg.datetime.day))
                    new_msg.set_named("hour", dballe.var("B04004", msg.datetime.hour))
                    new_msg.set_named("minute", dballe.var("B04005", msg.datetime.minute))
                    new_msg.set_named("second", dballe.var("B04006", msg.datetime.second))
                    new_msg.set_named("rep_memo", dballe.var("B01194", msg.report))
                    new_msg.set_named("ident", dballe.var("B01011", msg.ident))
                    new_msg.set_named("longitude", dballe.var("B06001", int(msg.coords[0]*10**5)))
                    new_msg.set_named("latitude", dballe.var("B05001", int(msg.coords[1]*10**5)))

                    for data in msg.query_data({"query": "attrs"}):
                        variable = data["variable"]
                        attrs = variable.get_attrs()
                        is_ok = pass_qc(attrs)
                        v = dballe.var(data["variable"].code, data["variable"].get())

                        if args.preserve and not is_ok:
                            v.seta(dballe.var("B33007", 0))
                        elif is_ok:
                            for a in attrs:
                                v.seta(a)
                        else:
                            continue

                        new_msg.set(data["level"], data["trange"], v)

                    for data in msg.query_station_data({"query": "attrs"}):
                        variable = data["variable"]
                        attrs = variable.get_attrs()
                        v = dballe.var(data["variable"].code, data["variable"].get())
                        for a in attrs:
                            v.seta(a)

                        new_msg.set(dballe.Level(), dballe.Trange(), v)

                    sys.stdout.buffer.write(exporter.to_binary(new_msg))
