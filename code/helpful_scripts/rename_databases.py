import database


databases = {'full_first': {'7': '1546470185805.0d38c0d1-2e4e-40ec-8628-e1565a9ddd52.full_first',
                            '6': '1546625250751.f19d18e3-fdea-43c5-9b8a-77153da3d49d.full_first',
                            '5': '1546712516993.a7e3769c-9ac0-4774-9544-8751f9bf39da.full_first',
                            '4': '1546779282878.2b8b88ec-819f-43ac-b392-42c1dd1e6fab.full_first',
                            '3': '1546810568680.46028467-af72-4e3b-88a4-268d12b205ac.full_first',
                            '2': '1546842311401.7815d1ff-9a5f-4e69-9017-dd7fbf07f24f.full_first',
                            '1': '1546870527873.cf09bfde-de13-4276-8601-4434123ac395.full_first'
                            },
             'full_last': {'7': '1546473641177.0d38c0d1-2e4e-40ec-8628-e1565a9ddd52.full_last',
                            '6': '1546628775168.f19d18e3-fdea-43c5-9b8a-77153da3d49d.full_last',
                            '5': '1546718087114.a7e3769c-9ac0-4774-9544-8751f9bf39da.full_last',
                            '4': '1546783151109.2b8b88ec-819f-43ac-b392-42c1dd1e6fab.full_last',
                            '3': '1546813572765.46028467-af72-4e3b-88a4-268d12b205ac.full_last',
                            '2': '1546845738097.7815d1ff-9a5f-4e69-9017-dd7fbf07f24f.full_last',
                            '1': '1546875384902.cf09bfde-de13-4276-8601-4434123ac395.full_last'
                            },
             'avg_full': {'7': '1546477551730.0d38c0d1-2e4e-40ec-8628-e1565a9ddd52.avg_full',
                           '6': '1546658603667.e74710f1-0486-4167-a7ef-81508c9c446d.avg_full',
                           '5': '1546727922602.a7e3769c-9ac0-4774-9544-8751f9bf39da.avg_full',
                           '4': '1546791218721.2b8b88ec-819f-43ac-b392-42c1dd1e6fab.avg_full',
                           '3': '1546821643112.46028467-af72-4e3b-88a4-268d12b205ac.avg_full',
                           '2': '1546853463017.8ef9f28b-bb63-432e-bc41-1da871166b35.avg_full',
                           '1': '1546881635662.cf09bfde-de13-4276-8601-4434123ac395.avg_full'
                           },
             'sum_full': {'7': '1546512833970.4672e0a1-dd40-4143-bf66-09a87237215a.sum_full',
                           '6': '1546635163606.520d6eb2-5bc8-4dea-bc33-2bff0c2d219a.sum_full',
                           '5': '1546722859265.a7e3769c-9ac0-4774-9544-8751f9bf39da.sum_full',
                           '4': '1546786820989.2b8b88ec-819f-43ac-b392-42c1dd1e6fab.sum_full',
                           '3': '1546817472661.46028467-af72-4e3b-88a4-268d12b205ac.sum_full',
                           '2': '1546849071335.7815d1ff-9a5f-4e69-9017-dd7fbf07f24f.sum_full',
                           '1': '1546878989223.cf09bfde-de13-4276-8601-4434123ac395.sum_full'
                           },
             'count_full': {'7': '1546518552965.4672e0a1-dd40-4143-bf66-09a87237215a.count_full',
                           '6': '1546640509139.520d6eb2-5bc8-4dea-bc33-2bff0c2d219a.count_full',
                           '5': '1546733906658.a7e3769c-9ac0-4774-9544-8751f9bf39da.count_full',
                           '4': '1546795029483.2b8b88ec-819f-43ac-b392-42c1dd1e6fab.count_full',
                           '3': '1546825433183.46028467-af72-4e3b-88a4-268d12b205ac.count_full',
                           '2': '1546857805274.8ef9f28b-bb63-432e-bc41-1da871166b35.count_full',
                           '1': '1546903119034.b322c0a4-9ca5-4b0f-b777-6689965ca065.count_full'
                           },
             'max_full': {'7': '1546526744085.4672e0a1-dd40-4143-bf66-09a87237215a.max_full',
                           '6': '1546645905278.520d6eb2-5bc8-4dea-bc33-2bff0c2d219a.max_full',
                           '5': '1546739838278.a7e3769c-9ac0-4774-9544-8751f9bf39da.max_full',
                           '4': '1546799029147.2b8b88ec-819f-43ac-b392-42c1dd1e6fab.max_full',
                           '3': '1546829869136.46028467-af72-4e3b-88a4-268d12b205ac.max_full',
                           '2': '1546862056343.8ef9f28b-bb63-432e-bc41-1da871166b35.max_full',
                           '1': '1546903311912.b322c0a4-9ca5-4b0f-b777-6689965ca065.max_full'
                           },
             'min_full': {'7': '1546532849839.4672e0a1-dd40-4143-bf66-09a87237215a.min_full',
                           '6': '1546652434529.520d6eb2-5bc8-4dea-bc33-2bff0c2d219a.min_full',
                           '5': '1546745156105.a7e3769c-9ac0-4774-9544-8751f9bf39da.min_full',
                           '4': '1546803249732.2b8b88ec-819f-43ac-b392-42c1dd1e6fab.min_full',
                           '3': '1546835306889.46028467-af72-4e3b-88a4-268d12b205ac.min_full',
                           '2': '1546865336814.8ef9f28b-bb63-432e-bc41-1da871166b35.min_full',
                           '1': '1546907167231.b322c0a4-9ca5-4b0f-b777-6689965ca065.min_full'
                           },
             'part_last': {'7': '1546551952239.3b8d7b0c-fb24-402d-9892-7ca271a75434.part_last',
                            '6': '1546655689492.ae293a25-9029-4a10-9cdd-8661ed729706.part_last',
                            '5': '1546712544769.3d3652d0-4fc6-4cf7-a79a-62a4728003ad.part_last',
                            '4': '1546779273698.2d674d62-5baf-44cb-8115-f15313c224d8.part_last',
                            '3': '1546810591644.37f1ea13-307d-4da8-9514-03b345657991.part_last',
                            '2': '1546842329170.cae5ed5b-7df8-48ea-8241-e74ad6a29286.part_last',
                            '1': '1546870554094.f89049e9-f38e-4f56-95bd-bbab2dc24581.part_last'
                            },
             'avg_part': {'7': '1546551954633.3b8d7b0c-fb24-402d-9892-7ca271a75434.avg_part',
                           '6': '1546655696278.ae293a25-9029-4a10-9cdd-8661ed729706.avg_part',
                           '5': '1546712551061.3d3652d0-4fc6-4cf7-a79a-62a4728003ad.avg_part',
                           '4': '1546779289964.2d674d62-5baf-44cb-8115-f15313c224d8.avg_part',
                           '3': '1546810604979.37f1ea13-307d-4da8-9514-03b345657991.avg_part',
                           '2': '1546842385531.cae5ed5b-7df8-48ea-8241-e74ad6a29286.avg_part',
                           '1': '1546870574467.f89049e9-f38e-4f56-95bd-bbab2dc24581.avg_part'
                           },
             'sum_part': {'7': '1546556665744.3b8d7b0c-fb24-402d-9892-7ca271a75434.sum_part',
                           '6': '1546657158924.ae293a25-9029-4a10-9cdd-8661ed729706.sum_part',
                           '5': '1546713872649.3d3652d0-4fc6-4cf7-a79a-62a4728003ad.sum_part',
                           '4': '1546783679545.2d674d62-5baf-44cb-8115-f15313c224d8.sum_part',
                           '3': '1546814275476.37f1ea13-307d-4da8-9514-03b345657991.sum_part',
                           '2': '1546844954232.cae5ed5b-7df8-48ea-8241-e74ad6a29286.sum_part',
                           '1': '1546871441117.f89049e9-f38e-4f56-95bd-bbab2dc24581.sum_part'
                           },
             'count_part': {'7': '1546560663321.3b8d7b0c-fb24-402d-9892-7ca271a75434.count_part',
                           '6': '1546657792813.ae293a25-9029-4a10-9cdd-8661ed729706.count_part',
                           '5': '1546714370819.3d3652d0-4fc6-4cf7-a79a-62a4728003ad.count_part',
                           '4': '1546790843922.2d674d62-5baf-44cb-8115-f15313c224d8.count_part',
                           '3': '1546815037334.37f1ea13-307d-4da8-9514-03b345657991.count_part',
                           '2': '1546850510184.cae5ed5b-7df8-48ea-8241-e74ad6a29286.count_part',
                           '1': '1546871627126.f89049e9-f38e-4f56-95bd-bbab2dc24581.count_part'
                           },
             'max_part': {'7': '1546564630956.3b8d7b0c-fb24-402d-9892-7ca271a75434.max_part',
                           '6': '1546657834977.ae293a25-9029-4a10-9cdd-8661ed729706.max_part',
                           '5': '1546714772898.3d3652d0-4fc6-4cf7-a79a-62a4728003ad.max_part',
                           '4': '1546794754026.2d674d62-5baf-44cb-8115-f15313c224d8.max_part',
                           '3': '1546818357062.37f1ea13-307d-4da8-9514-03b345657991.max_part',
                           '2': '1546852353424.cae5ed5b-7df8-48ea-8241-e74ad6a29286.max_part',
                           '1': '1546871791287.f89049e9-f38e-4f56-95bd-bbab2dc24581.max_part'
                           },
             'min_part': {'7': '1546568656613.3b8d7b0c-fb24-402d-9892-7ca271a75434.min_part',
                           '6': '1546661806137.ae293a25-9029-4a10-9cdd-8661ed729706.min_part',
                           '5': '1546718905841.3d3652d0-4fc6-4cf7-a79a-62a4728003ad.min_part',
                           '4': '1546798690187.2d674d62-5baf-44cb-8115-f15313c224d8.min_part',
                           '3': '1546819621003.37f1ea13-307d-4da8-9514-03b345657991.min_part',
                           '2': '1546853398921.6498357b-0da8-417b-9a02-9ef9433c2fde.min_part',
                           '1': '1546871950339.f89049e9-f38e-4f56-95bd-bbab2dc24581.min_part'
                           },

             }

client = database.Client()

for db in databases:
    for experiment in databases[db]:
        new_database = databases[db][experiment] + '%' + experiment
        old_database = databases[db][experiment]
        client.switch_database(old_database)
        for measurement in client.show_measurements():
            client.switch_database(old_database)
            t = client.query('select * from ' + measurement['name'])
            writable_data = []
            for data in t:
                writable_data.append({'tags': {'driver': data['driver']}, 'fields': {'result': data['result'],
                                                                                     'metadata': data['metadata']},
                                      'measurement': measurement['name'], 'time': data['time']})
            client.switch_database(new_database)
            client.write_records(writable_data)
        client.delete_database(old_database)
