import pyarrow as pa
import pyarrow.parquet as pq

def func():
    countries = []
    populations = []

    countries.append('Sweden')
    populations.append([{'city': 'Stockholm', 'population': 1515017},
                        {'city': 'Gothenburg', 'population': 590580}])
    countries.append('Norway')
    populations.append([{'city': 'Oslo', 'population': 958378},
                        {'city': 'Bergen', 'population': 254235}])


    ty = pa.struct([pa.field('city', pa.string()),
                    pa.field('population', pa.int32())
    ])

    fields = [
        pa.field('country', pa.string()),
        pa.field('populations', pa.list_(ty)),
    ]
    sch1 = pa.schema(fields)

    data = [
        pa.array(countries),
        pa.array(populations, type=pa.list_(ty))
    ]
    batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1'])
    table = pa.Table.from_batches([batch], sch1)
    writer = pq.ParquetWriter('cities.parquet', sch1)
    writer.write_table(table)
    writer.close()

def main():
    func()

if __name__ == '__main__':
    main()
