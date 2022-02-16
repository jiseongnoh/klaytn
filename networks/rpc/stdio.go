// Modifications Copyright 2018 The klaytn Authors
// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.
//
// This file is derived from rpc/client.go (2018/06/04).
// Modified and improved for the klaytn development.

package rpc

import (
	"context"
	"net"
	"os"
	"time"

	"github.com/pkg/errors"
)

type StdIOConn struct{}

func (io StdIOConn) Read(b []byte) (n int, err error) {
	return os.Stdin.Read(b)
}

func (io StdIOConn) Write(b []byte) (n int, err error) {
	return os.Stdout.Write(b)
}

func (io StdIOConn) Close() error {
	return nil
}

func (io StdIOConn) LocalAddr() net.Addr {
	return &net.UnixAddr{Name: "stdio", Net: "stdio"}
}

func (io StdIOConn) RemoteAddr() net.Addr {
	return &net.UnixAddr{Name: "stdio", Net: "stdio"}
}

func (io StdIOConn) SetDeadline(t time.Time) error {
	return &net.OpError{Op: "set", Net: "stdio", Source: nil, Addr: nil, Err: errors.New("deadline not supported")}
}

func (io StdIOConn) SetReadDeadline(t time.Time) error {
	return &net.OpError{Op: "set", Net: "stdio", Source: nil, Addr: nil, Err: errors.New("deadline not supported")}
}

func (io StdIOConn) SetWriteDeadline(t time.Time) error {
	return &net.OpError{Op: "set", Net: "stdio", Source: nil, Addr: nil, Err: errors.New("deadline not supported")}
}
func DialStdIO(ctx context.Context) (*Client, error) {
	return NewClient(ctx, func(_ context.Context) (net.Conn, error) {
		return StdIOConn{}, nil
	})
}
