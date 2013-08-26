// Copyright 2013 Tahir Hashmi, Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redis

import (
	"errors"
	"sync/atomic"
)

// The policy to be used for selecting a slave from which to get a connection
type SlaveSelectionPolicy int

const (
	RoundRobin SlaveSelectionPolicy = iota
)

// Cluster represents a set of 1 Redis Master and 1 or more slaves of
// that master. Master and slaves are represented in the cluster by a
// Pool for the node required.
//
// The application needs to be explicit about whether it wants a
// master or slave connection. It is also up to the application to
// ensure that writes don't go to the master.
type Cluster struct {
	Policy    SlaveSelectionPolicy
	rrCounter uint32 // Counter for deciding which slave is next in a Round-Robin policy
	master    *Pool
	slaves    []*Pool
}

var ErrMasterAssigned = errors.New("A master has already been assigned. Use Cluster.replaceMaster to replace")
var ErrNilPool = errors.New("Given Pool(s) is not initialized")

// Add a master to the cluster. Read-only applications can skip this
// call to have only slaves in their cluster
func (c *Cluster) AddMaster(p *Pool) error {
	if c.master != nil {
		return ErrMasterAssigned
	}
	if p == nil {
		return ErrNilPool
	}
	c.master = p
	return nil
}

// Close all connections to the current master and replace it with a
// new master pool
func (c *Cluster) replaceMaster(p *Pool) error {
	if p == nil {
		return ErrNilPool
	}
	if c.master != nil {
		c.master.Close()
	}
	c.master = p
	return nil
}

// Add a slave to the cluster. The same slave pool can be added
// repeatedly to give it more weightage in the scheduling policy
func (c *Cluster) AddSlave(p *Pool) error {
	if p == nil {
		return ErrNilPool
	}
	if c.slaves == nil {
		c.slaves = make([]*Pool, 0)
	}

	c.slaves = append(c.slaves, p)
	return nil
}

// Set multiple slaves in the cluster at once. Existing slave pools
// will be closed and replaced.
func (c *Cluster) SetSlaves(pl []*Pool) error {
	for _, pool := range pl {
		if pool == nil {
			return ErrNilPool
		}
	}
	if c.slaves != nil && len(c.slaves) > 0 {
		for _, slave := range c.slaves {
			slave.Close()
		}
		c.slaves = nil
	}
	c.slaves = pl
	return nil
}

// Get a pooled connection from the master
func (c *Cluster) GetMasterConn() Conn {
	return c.master.Get()
}

// Get a pooled connection from one of the slaves as per the rotation
// policy configured in Cluster.Policy.
func (c *Cluster) GetSlaveConn() Conn {
	if c.slaves == nil || len(c.slaves) == 0 {
		return nil
	}
	if c.Policy == RoundRobin {
		i := atomic.AddUint32(&(c.rrCounter), 1)
		slaveIdx := i % uint32(len(c.slaves))
		return c.slaves[slaveIdx].Get()
	}
	return nil
}

// Close all pools and remove everything from the cluster
func (c *Cluster) TearDown() {
	if c.master != nil {
		c.master.Close()
		c.master = nil
	}
	if c.slaves != nil && len(c.slaves) > 0 {
		for _, slave := range c.slaves {
			slave.Close()
		}
		c.slaves = nil
	}
}
