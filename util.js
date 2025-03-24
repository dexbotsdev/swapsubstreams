import axios from 'axios'
export const objectToJsonString = obj => {
    /*
        Workaround because JS built-in function is not able to convert BigInt
        https://github.com/GoogleChromeLabs/jsbi/issues/30
    */
    BigInt.prototype.toJSON = function() { return this.toString() }

    return JSON.stringify(obj)
}


const main= async()=>{

    const data = await fetch('https://data.solanatracker.io/search?query=7wDpth99D7NDXq917WBfgp4npJ59UTd3xg11mHi4pump',
        {headers:{'x-api-key':'8a51c8a2-5f09-4641-83af-d0727d103b81'}}
    );

    const a = await data.json();

    console.log(a)
}


main();