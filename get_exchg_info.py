import requests, sys
sys.path += ['/home/ec2-user/tools']
import MyTools as mt


def get_exchg(inst_type):
    # ofile

    ofile = f'./data/ExchangeInof_{inst_type}.json'

    # Define the endpoint URL
    
    bp = mt.get_basepoint('Binance', inst_type)
    url = f"{bp}exchangeInfo"


    #url = f"{bp}exchangeInfo"

    # Make the GET request
    response = requests.get(url)


    # Check if the request was successful
    if response.status_code == 200:
        # Print the response content (kline data)
        print(type(response))
        #print(response.json())
        mt.write(response.json(), ofile)

    else:
        # Print error message if request was not successful
        print("Error:", response.status_code, response.text)
    
    # to remove later
    data = mt.open_json(ofile)
    datas = data['symbols']
    for data in datas:
        ie = data['symbol']
        if 'BTC' in ie:
            #print(ie, "|", data['status'],"|", data['contractType'])
            print(data)
            print("===" * 50 + "\n\n\n")

    return response.json()
    



def main():
    get_exchg('SPOT')
    get_exchg('UFUTURE')

    


if __name__=="__main__":
    main()

