package core

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/klaytn/klaytn/blockchain/types"
	"github.com/klaytn/klaytn/common"
	"github.com/klaytn/klaytn/consensus/istanbul"
	mock_istanbul "github.com/klaytn/klaytn/consensus/istanbul/mocks"
	"github.com/klaytn/klaytn/fork"
	"github.com/klaytn/klaytn/params"
)

func TestCore_sendPrepare(t *testing.T) {
	enableLog()
	fork.SetHardForkBlockNumberConfig(&params.ChainConfig{})
	defer fork.ClearHardForkBlockNumberConfig()

	validatorAddrs, validatorKeyMap := genValidators(6)
	mockBackend, mockCtrl := newMockBackend(t, validatorAddrs)
	//mockBackend1, mockCtrl1 := newMockBackend(t, validatorAddrs)

	istConfig := istanbul.DefaultConfig
	istConfig.ProposerPolicy = istanbul.WeightedRandom

	istCore := New(mockBackend, istConfig).(*core)
	if err := istCore.Start(); err != nil {
		t.Fatal(err)
	}
	defer istCore.Stop()

	//istCore1 := New(mockBackend1, istConfig).(*core)
	//if err := istCore1.Start(); err != nil {
	//	t.Fatal(err)
	//}
	//defer istCore1.Stop()

	lastProposal, lastProposer := mockBackend.LastProposal()
	proposal, err := genBlock(lastProposal.(*types.Block), validatorKeyMap[validatorAddrs[0]])
	if err != nil {
		t.Fatal(err)
	}

	istCore.current.Preprepare = &istanbul.Preprepare{
		View:     istCore.currentView(),
		Proposal: proposal,
	}
	//istCore1.current.Preprepare = &istanbul.Preprepare{
	//	View:     istCore1.currentView(),
	//	Proposal: proposal,
	//}

	mockCtrl.Finish()
	//mockCtrl1.Finish()

	//// invalid case - not committee
	//{
	//	// Increase round number until the owner of istanbul.core is not a member of the committee
	//	for istCore.valSet.CheckInSubList(lastProposal.Hash(), istCore.currentView(), istCore.Address()) {
	//		istCore.current.round.Add(istCore.current.round, common.Big1)
	//		istCore.valSet.CalcProposer(lastProposer, istCore.current.round.Uint64())
	//	}
	//
	//	for istCore1.valSet.CheckInSubList(lastProposal.Hash(), istCore1.currentView(), istCore.Address()) {
	//		istCore1.current.round.Add(istCore1.current.round, common.Big1)
	//		istCore1.valSet.CalcProposer(lastProposer, istCore1.current.round.Uint64())
	//	}
	//
	//	mockCtrl := gomock.NewController(t)
	//	mockBackend := mock_istanbul.NewMockBackend(mockCtrl)
	//	mockBackend.EXPECT().Sign(gomock.Any()).Return(nil, nil).Times(0)
	//	mockBackend.EXPECT().Broadcast(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(0)
	//
	//	mockCtrl1 := gomock.NewController(t)
	//	mockBackend1 := mock_istanbul.NewMockBackend(mockCtrl)
	//	mockBackend1.EXPECT().Sign(gomock.Any()).Return(nil, nil).Times(0)
	//	mockBackend1.EXPECT().Broadcast(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(0)
	//
	//	istCore.backend = mockBackend
	//	istCore.sendPrepare()
	//
	//	istCore1.backend = mockBackend1
	//	istCore1.sendPrepare()
	//
	//	// methods of mockBackend should be executed given times
	//	mockCtrl.Finish()
	//	mockCtrl1.Finish()
	//}

	// valid case
	{
		// Increase round number until the owner of istanbul.core become a member of the committee
		for !istCore.valSet.CheckInSubList(lastProposal.Hash(), istCore.currentView(), istCore.Address()) {
			istCore.current.round.Add(istCore.current.round, common.Big1)
			istCore.valSet.CalcProposer(lastProposer, istCore.current.round.Uint64())
		}
		//for !istCore1.valSet.CheckInSubList(lastProposal.Hash(), istCore1.currentView(), istCore1.Address()) {
		//	istCore1.current.round.Add(istCore1.current.round, common.Big1)
		//	istCore1.valSet.CalcProposer(lastProposer, istCore1.current.round.Uint64())
		//}

		mockCtrl := gomock.NewController(t)
		mockBackend := mock_istanbul.NewMockBackend(mockCtrl)
		mockBackend.EXPECT().Sign(gomock.Any()).Return(nil, nil).Times(1)
		mockBackend.EXPECT().Broadcast(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

		istCore.backend = mockBackend
		istCore.sendPrepare()

		//mockCtrl1 := gomock.NewController(t)
		//mockBackend1 := mock_istanbul.NewMockBackend(mockCtrl1)
		//mockBackend1.EXPECT().Sign(gomock.Any()).Return(nil, nil).Times(1)
		//mockBackend1.EXPECT().Broadcast(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

		//istCore1.backend = mockBackend1
		//istCore1.sendPrepare()

		// methods of mockBackend should be executed given times
		mockCtrl.Finish()
		//mockCtrl1.Finish()

	}
}
