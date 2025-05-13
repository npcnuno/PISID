## pisid_mazeact

import time
from decision import Decision
from mySql import MySql

def main() -> None:


    decision = Decision( "pisid_mazeact", 33 )
    mySql = MySql( 33 )

    decision.close_all_doors()
    time.sleep( 5 )
    print( "Starting" )

    while True:
    
        decision.close_all_doors()
        time.sleep( 4 )

        for sala in range( 0, 9 ):

            data = mySql.get_medicoes_by_room( sala )

            handle_room( data, sala, mySql.idJogo, decision )

            print( '-------------' )

        decision.open_all_doors()
        time.sleep( 2 )

def handle_room( data, room, jogoId, decision ):

    if len( data ) == 0:
        return

    even = []
    odd = []

    for d in data:

        if d[ -1 ] != jogoId:

            continue

        if d[ 4 ] % 2 == 0:

            even.append( d[ 4 ] )

        else:

            odd.append( d[ 4 ] )

    if len( set( even ) ) == len( set( odd ) ) != 0:

        print( odd )
        print( even )
        print( "Score in room: " + str( room ) )
        # time.sleep( 60 )
        decision.send_score( str( room ) )
        return True

    return False

if __name__ == "__main__":
    main()
