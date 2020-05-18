const { DataTypes } = require('sequelize');

module.exports = function (sequelize) {
  const Cache = sequelize.define(
    'Cache',
    {
      id: {
        type: DataTypes.STRING,
        primaryKey: true,
      },
      name: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      data: {
        type: DataTypes.JSON,
      },
      blob: {
        type: DataTypes.BLOB,
      },
      text: {
        type: DataTypes.TEXT,
      },
      expiryDate: {
        type: DataTypes.DATE,
        allowNull: false,
      },
      createdAt: {
        type: DataTypes.DATE,
        allowNull: false,
      },
    },
    {
      tableName: 'cache',
      underscored: true,
      updatedAt: false,
    }
  );

  return Cache;
};
