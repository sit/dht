#
# Common code for hopcount running scripts.
#
PERLLIB=$SRCROOT/tst
export PERLLIB
outdir=./`basename $0`-$$

echo "Testing $ROUTING_MODE with $NUMVNODES vnodes per lsd; $NUMLSDS total."
$SRCROOT/tst/hopcount $outdir $BUILDROOT $NUMLSDS $NUMVNODES $ROUTING_MODE
exitstatus=$?

if test $exitstatus -eq 0;
then
    rm -r $outdir
fi

exit $exitstatus

