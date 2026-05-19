export const getPackageAmountByPackageId = (packageId: number) => {
  switch (packageId) {
    case 1:
      return 5;
    case 2:
      return 10;
    case 3:
      return 20;
    case 4:
      return 40;
    case 5:
      return 80;
    case 6:
      return 160;
    case 7:
      return 320;
    case 8:
      return 640;
    case 9:
      return 1280;
    case 10:
      return 2560;
    case 11:
      return 5120;
    case 12:
      return 10240;
    default:
      return 0;
  }
};

export const getPackageNumberByLevelAmount = (amount: string) => {
  switch (amount) {
    case '100000000000000000':
      return 1;
    case '150000000000000000':
      return 1;
    case '200000000000000000':
      return 2;
    case '300000000000000000':
      return 2;
    case '400000000000000000':
      return 3;
    case '600000000000000000':
      return 3;
    case '800000000000000000':
      return 4;
    case '1200000000000000000':
      return 4;
    case '1600000000000000000':
      return 5;
    case '2400000000000000000':
      return 5;
    case '3200000000000000000':
      return 6;
    case '4800000000000000000':
      return 6;
    case '6400000000000000000':
      return 7;
    case '9600000000000000000':
      return 7;
    case '12800000000000000000':
      return 8;
    case '19200000000000000000':
      return 8;
    case '25600000000000000000':
      return 9;
    case '38400000000000000000':
      return 9;
    case '51200000000000000000':
      return 10;
    case '76800000000000000000':
      return 10;
    case '102400000000000000000':
      return 11;
    case '153600000000000000000':
      return 11;
    case '204800000000000000000':
      return 12;
    case '307200000000000000000':
      return 12;
    default:
      return 0;
  }
};

export const getPackageNumberByMatrixAmount = (amount: string) => {
  switch (amount) {
    case '2500000000000000000':
      return 1;
    case '5000000000000000000':
      return 2;
    case '10000000000000000000':
      return 3;
    case '20000000000000000000':
      return 4;
    case '40000000000000000000':
      return 5;
    case '80000000000000000000':
      return 6;
    case '160000000000000000000':
      return 7;
    case '320000000000000000000':
      return 8;
    case '640000000000000000000':
      return 9;
    case '1280000000000000000000':
      return 10;
    case '2560000000000000000000':
      return 11;
    case '5120000000000000000000':
      return 12;
    default:
      return 0;
  }
};
