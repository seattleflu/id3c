-- Verify seattleflu/schema:receiving/presence-absence/document-validation on pg

begin;

do $$
    begin
        insert into receiving.presence_absence (document) values ('
            {
              "calls": [
                {
                  "source": {
                    "sample": "20e1d4a2",
                    "aliquot": "3492492"
                  },
                  "target": {
                    "name": "FluA"
                  },
                  "present": true
                },
                {
                  "source": {
                    "sample": "20e1d4a2",
                    "aliquot": "3492492"
                  },
                  "target": {
                    "name": "FluB"
                  },
                  "present": null
                }
              ]
            }
        ');

        begin
            insert into receiving.presence_absence (document) values ('[]');
            assert false, 'insert of invalid value succeeded';
        exception
            when check_violation then null; -- expected
        end;

        begin
            insert into receiving.presence_absence (document) values ('{}');
            assert false, 'insert of invalid value succeeded';
        exception
            when check_violation then null; -- expected
        end;

        begin
            insert into receiving.presence_absence (document) values ('{"calls":[]}');
            assert false, 'insert of invalid value succeeded';
        exception
            when check_violation then null; -- expected
        end;
    end
$$;

rollback;
