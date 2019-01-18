

Si può far partire il Server e testarlo con make consegna, oppure utilizzare 

gcc -g membox.c connections.c icl_hash.c thread_pool.c -pthread -pedantic -Wall -o membox

per compilare e testare le singole immissioni con la sintassi standard.

E' stato modificato il Client.c, commentando la riga che scrive "successo" quando riesce
a completare un'operazione, per rendere più leggibili i test.
