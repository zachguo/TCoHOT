# interactive one-liner to be run from the shell prompt
# run against all the *METS.xml files in a given folder, e.g 'out_aa' here
# egrep the beginning xml tag(s) you want, separate multiples by pipe (|)
# output or append (>>) to a file, e.g. 'outaa.xml' here

cat ./out_aa/*.xml | egrep '(PREMIS:objectIdentifierValue|controlfield tag="008")' >> outaa.xml
