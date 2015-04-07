package databasetemplate

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewMasterSlaveConfig(t *testing.T) {
	jsonStr := `{"master":{"user":"th_dev","passwd":"th_devpass","database":"tapalliance_1_1","host":"localhost"},"slave":[]}`
	masterSlaveConfig, ok := NewMasterSlaveConfig(jsonStr)
	assert.True(t, ok)
	assert.NotEmpty(t, masterSlaveConfig.Master.Host)
	fmt.Println(masterSlaveConfig.Master.Host)

}

func TestNewMasterSlaveConfig2(t *testing.T) {
	jsonStr := `{"master":{"user":"th_dev","passwd":"th_devpass","database":"tapalliance_1_1","host":"localhost"},"slave":[{"user":"th_dev","passwd":"th_devpass","database":"tapalliance_1_1","host":"localhost"},{"user":"th_dev","passwd":"th_devpass","database":"tapalliance_1_1","host":"localhost"}]}`
	masterSlaveConfig, ok := NewMasterSlaveConfig(jsonStr)
	assert.True(t, ok)
	assert.NotEmpty(t, masterSlaveConfig.Master.Host)
	fmt.Println(masterSlaveConfig.Master.Host)
	assert.Equal(t, 2, len(masterSlaveConfig.SlaveList))

}

func TestNewShardingConfig(t *testing.T) {
	jsonStr := `{"sharding_length":1,"sharding":[{"master":{"user":"th_dev","passwd":"th_devpass","database":"tapalliance_1_1","host":"localhost"},"slave":[]}]}`
	config, ok := NewShardingConfig(jsonStr)
	assert.True(t, ok)
	assert.Equal(t, 1, len(config.MasterList))
	assert.Equal(t, config.MasterListLength, len(config.MasterList))
	assert.NotEmpty(t, len(config.MasterList[0].Master.Host))
	assert.NotEmpty(t, len(config.MasterList[0].Master.Host))

}
