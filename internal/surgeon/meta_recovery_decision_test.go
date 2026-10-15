package surgeon_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.etcd.io/bbolt/internal/surgeon"
)

func validInspection(pageID, txid uint64) surgeon.MetaPageInspection {
	return surgeon.MetaPageInspection{
		PageID:      pageID,
		Readable:    true,
		MagicOK:     true,
		VersionOK:   true,
		ChecksumOK:  true,
		Txid:        txid,
		Fingerprint: fingerprintFor(pageID, txid),
	}
}

// fingerprintFor synthesizes distinct, deterministic fingerprints for the
// pure decision table (fingerprints only need to compare equal/different).
func fingerprintFor(pageID, txid uint64) string {
	b := make([]byte, 64)
	for i := range b {
		b[i] = byte(pageID)*16 + byte(txid) + byte(i)
	}
	return hexBytes(b)
}

func hexBytes(b []byte) string {
	const h = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[2*i] = h[v>>4]
		out[2*i+1] = h[v&0xf]
	}
	return string(out)
}

func TestDecideMetaRecovery(t *testing.T) {
	t.Run("both invalid aborts with both reasons", func(t *testing.T) {
		insp := [2]surgeon.MetaPageInspection{
			{PageID: 0, Readable: true, ValidateError: "checksum error"},
			{PageID: 1, Readable: true, ValidateError: "invalid database"},
		}
		_, err := surgeon.DecideMetaRecovery(insp, nil)
		require.Error(t, err)
		assert.True(t, errors.Is(err, surgeon.ErrBothMetaPagesInvalid))
		assert.Contains(t, err.Error(), "checksum error")
		assert.Contains(t, err.Error(), "invalid database")
	})

	t.Run("sole valid page zero", func(t *testing.T) {
		bad := validInspection(1, 11)
		bad.ValidateError = "checksum error"
		bad.ChecksumOK = false
		insp := [2]surgeon.MetaPageInspection{validInspection(0, 10), bad}
		d, err := surgeon.DecideMetaRecovery(insp, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, d.SelectedPage)
		assert.Equal(t, surgeon.ReasonOnlyValid, d.Reason)
		assert.Equal(t, surgeon.ActionCopyPage, d.Action.Type)
		assert.EqualValues(t, 0, d.Action.From)
		assert.EqualValues(t, 1, d.Action.To)
		require.NotNil(t, d.Rollback)
		assert.EqualValues(t, 11, d.Rollback.DiscardedTxid)
	})

	t.Run("sole valid page one with lower txid is not a rollback", func(t *testing.T) {
		bad := validInspection(0, 8)
		bad.ValidateError = "checksum error"
		bad.ChecksumOK = false
		insp := [2]surgeon.MetaPageInspection{bad, validInspection(1, 9)}
		d, err := surgeon.DecideMetaRecovery(insp, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, d.SelectedPage)
		assert.Nil(t, d.Rollback)
	})

	t.Run("higher txid wins", func(t *testing.T) {
		insp := [2]surgeon.MetaPageInspection{validInspection(0, 9), validInspection(1, 10)}
		d, err := surgeon.DecideMetaRecovery(insp, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, d.SelectedPage)
		assert.Equal(t, surgeon.ReasonHigherTxid, d.Reason)
		assert.EqualValues(t, 1, d.Action.From)
		assert.EqualValues(t, 0, d.Action.To)
		assert.Nil(t, d.Rollback)
	})

	t.Run("identical needs no action", func(t *testing.T) {
		m0 := validInspection(0, 10)
		m1 := validInspection(1, 10)
		m1.Fingerprint = m0.Fingerprint
		insp := [2]surgeon.MetaPageInspection{m0, m1}
		d, err := surgeon.DecideMetaRecovery(insp, nil)
		require.NoError(t, err)
		assert.Equal(t, -1, d.SelectedPage)
		assert.Equal(t, surgeon.ReasonIdentical, d.Reason)
		assert.Equal(t, surgeon.ActionNone, d.Action.Type)
	})

	t.Run("ambiguous without force aborts with diverging fields", func(t *testing.T) {
		m0 := validInspection(0, 10)
		m1 := validInspection(1, 10)
		m0.RootSequence = 1
		m1.RootSequence = 2
		insp := [2]surgeon.MetaPageInspection{m0, m1}
		_, err := surgeon.DecideMetaRecovery(insp, nil)
		require.Error(t, err)
		assert.True(t, errors.Is(err, surgeon.ErrMetaPagesAmbiguous))
		assert.Contains(t, err.Error(), "rootSequence")
	})

	t.Run("ambiguous forced selection", func(t *testing.T) {
		m0 := validInspection(0, 10)
		m1 := validInspection(1, 10)
		m0.RootSequence = 1
		m1.RootSequence = 2
		insp := [2]surgeon.MetaPageInspection{m0, m1}
		forced := 0
		d, err := surgeon.DecideMetaRecovery(insp, &forced)
		require.NoError(t, err)
		assert.Equal(t, 0, d.SelectedPage)
		assert.Equal(t, surgeon.ReasonOperatorForced, d.Reason)
		assert.EqualValues(t, 0, d.Action.From)
		assert.EqualValues(t, 1, d.Action.To)
		require.NotNil(t, d.Rollback)
		assert.Contains(t, d.DivergingFields[0], "rootSequence")
	})

	t.Run("invalid forced page", func(t *testing.T) {
		m0 := validInspection(0, 10)
		m1 := validInspection(1, 10)
		m0.RootSequence = 1
		m1.RootSequence = 2
		insp := [2]surgeon.MetaPageInspection{m0, m1}
		forced := 2
		_, err := surgeon.DecideMetaRecovery(insp, &forced)
		require.Error(t, err)
	})
}

func TestDecideMetaRecoveryIsDeterministic(t *testing.T) {
	insp := [2]surgeon.MetaPageInspection{validInspection(0, 9), validInspection(1, 10)}
	first, err := surgeon.DecideMetaRecovery(insp, nil)
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		d, err := surgeon.DecideMetaRecovery(insp, nil)
		require.NoError(t, err)
		assert.Equal(t, first, d)
	}
}
